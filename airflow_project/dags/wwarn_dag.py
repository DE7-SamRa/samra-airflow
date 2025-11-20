import logging
import re
from datetime import timedelta

import pandas as pd
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)

# 환경 설정
BUCKET_NAME = "samra-bucket"
AWS_REGION = "ap-northeast-2"


def parse_kma_format_response(response_text: str):
    """기상청 특수 형식 응답 파싱"""
    items = []
    lines = response_text.strip().split("\n")
    headers = []
    data_started = False

    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            continue

        # 헤더 파싱
        if not data_started and line_stripped.startswith("#"):
            if "REG_UP" in line_stripped and "TM_FC" in line_stripped:
                header_match = re.findall(
                    r"\bREG_UP\b|\bREG_UP_KO\b|\bREG_ID\b|\bREG_KO\b|"
                    r"\bTM_FC\b|\bTM_EF\b|\bWRN\b|\bLVL\b|\bCMD\b|\bED_TM\b",
                    line_stripped,
                )
                if header_match:
                    headers = header_match
                continue

        # 데이터 파싱
        if not line_stripped.startswith("#") and headers:
            data_started = True
            values = [v.strip() for v in line_stripped.split(",")]

            if len(values) >= len(headers):
                item = dict(zip(headers, values[: len(headers)], strict=False))
                items.append(item)
            elif len(values) == len(headers) - 1:
                item = dict(zip(headers[:-1], values, strict=False))
                item[headers[-1]] = ""
                items.append(item)
    return items


@dag(  # noqa: AIR311
    dag_id="kma_warning_pipeline",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2025, 1, 15, tz="Asia/Seoul"),
    catchup=False,
    tags=["kma", "weather", "snowflake", "image_split"],
)
def kma_warning_pipeline():
    # =================================================================
    # 텍스트 데이터 파이프라인
    # =================================================================

    @task(retries=3, retry_delay=timedelta(minutes=1))  # noqa: AIR311
    def extract_text():
        """특보 텍스트 API 호출"""
        url = "https://apihub.kma.go.kr/api/typ01/url/wrn_now_data.php"
        key = Variable.get("KMA_key", default_var="RReIhJQBRsuXiISUASbLPg")  # noqa: AIR311
        now = pendulum.now("Asia/Seoul")
        tm = now.strftime("%Y%m%d%H%M")

        params = {"fe": "f", "tm": tm, "help": 1, "authKey": key}

        try:
            # [수정] timeout을 30초에서 120초로 증가
            response = requests.get(url, params=params, timeout=120)
            if response.status_code != 200:
                raise ValueError(f"Text API Error: {response.status_code}")

            response.encoding = "euc-kr"

            items = parse_kma_format_response(response.text)

            if not items:
                logging.info("API 호출 성공했으나 파싱된 데이터가 없습니다.")
                return []

            logging.info(f"파싱된 데이터 건수: {len(items)}건")
            return items

        except Exception as e:
            logging.error(f"API Request Failed: {e}")
            raise

    @task  # noqa: AIR311
    def transform_text(items):
        """텍스트 전처리"""
        if not items:
            return []

        df = pd.DataFrame(items)
        required_cols = [
            "REG_UP",
            "REG_UP_KO",
            "REG_ID",
            "REG_KO",
            "TM_FC",
            "TM_EF",
            "WRN",
            "LVL",
            "CMD",
            "ED_TM",
        ]
        cols = [c for c in required_cols if c in df.columns]
        df = df[cols]

        for col in ["TM_FC", "TM_EF"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

        df["processed_at"] = pendulum.now("Asia/Seoul").to_datetime_string()
        df = df.where(pd.notnull(df), None)

        return df.to_dict("records")

    @task  # noqa: AIR311
    def load_text_to_s3(data_list, logical_date=None):
        """텍스트 데이터 CSV S3 적재"""
        if not data_list:
            raise AirflowSkipException("적재할 텍스트 데이터가 없습니다.")

        df = pd.DataFrame(data_list)

        if isinstance(logical_date, str):
            ts = pendulum.parse(logical_date).strftime("%Y%m%d_%H%M")
        else:
            ts = logical_date.strftime("%Y%m%d_%H%M")

        s3_key = f"raw_data/WWARN/wwarn_data/wwarn_{ts}.csv"
        csv_buffer = df.to_csv(index=False, encoding="utf-8-sig")

        hook = S3Hook(aws_conn_id="s3_key")
        hook.load_string(
            string_data=csv_buffer,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )

        logging.info(f"S3 Upload Success: {s3_key}")
        return s3_key

    # =================================================================
    # 이미지 데이터 파이프라인
    # =================================================================

    @task  # noqa: AIR311
    def process_image(logical_date=None):
        """이미지 API 호출 및 메타데이터 생성"""
        url = "https://apihub.kma.go.kr/api/typ03/cgi/wrn/nph-wrn7"
        key = Variable.get("KMA_key", default_var="RReIhJQBRsuXiISUASbLPg")  # noqa: AIR311

        if isinstance(logical_date, str):
            now = pendulum.parse(logical_date).in_timezone("Asia/Seoul")
        else:
            now = pendulum.now("Asia/Seoul")

        tm = now.strftime("%Y%m%d%H%M")
        file_ts = now.strftime("%Y%m%d_%H%M")

        params = {
            "tm": tm,
            "lon": "127.7",
            "lat": "36.1",
            "range": "300",
            "size": "685",
            "tmef": "1",
            "city": "1",
            "name": "0",
            "wrn": "W,R,C,D,O,V,T,S,Y,H",
            "authKey": key,
        }

        try:
            response = requests.get(url, params=params, stream=True, timeout=120)
            if response.status_code != 200:
                logging.error(f"Image API Error: {response.status_code}")
                return []

            img_s3_key = f"raw_data/WWARN/wwarn_images/wwarn_img_{file_ts}.png"

            hook = S3Hook(aws_conn_id="s3_key")

            s3_client = hook.get_conn()
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=img_s3_key,
                Body=response.content,
                ContentType='image/png',
                ContentDisposition='inline',
            )

            full_image_url = (
                f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{img_s3_key}"
            )

            meta_data = [
                {
                    "TM": tm,
                    "IMAGE_URL": full_image_url,
                    "CREATED_AT": now.to_datetime_string(),
                }
            ]
            return meta_data

        except Exception as e:
            logging.error(f"Image process failed: {e}")
            return []

    @task  # noqa: AIR311
    def load_img_meta_to_s3(meta_data, logical_date=None):
        """이미지 메타데이터 적재"""
        if not meta_data:
            raise AirflowSkipException("적재할 이미지 메타데이터가 없습니다.")

        df = pd.DataFrame(meta_data)

        if isinstance(logical_date, str):
            ts = pendulum.parse(logical_date).strftime("%Y%m%d_%H%M")
        else:
            ts = logical_date.strftime("%Y%m%d_%H%M")

        s3_key = f"raw_data/WWARN/wwarn_img_meta/img_meta_{ts}.csv"
        csv_buffer = df.to_csv(index=False, encoding="utf-8-sig")

        hook = S3Hook(aws_conn_id="s3_key")
        hook.load_string(
            string_data=csv_buffer,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        logging.info(f"Image Meta CSV Uploaded: {s3_key}")
        return s3_key

    # =================================================================
    # [Snowflake Operators]
    # =================================================================

    load_text_snowflake = SQLExecuteQueryOperator(
        task_id="load_text_snowflake",
        conn_id="snowflake_conn_id",
        sql=(
            "CREATE OR REPLACE TEMPORARY TABLE SAMRA.RAW_DATA.WARNING_STATUS_TEMP "
            "LIKE SAMRA.RAW_DATA.WARNING_STATUS;\n"

            "COPY INTO SAMRA.RAW_DATA.WARNING_STATUS_TEMP\n"
            "FROM @SAMRA.PUBLIC.WWARN_STAGE/"
            "{{ ti.xcom_pull(task_ids='load_text_to_s3') }}\n"
            "FILE_FORMAT = (TYPE = 'CSV' PARSE_HEADER = TRUE "
            "NULL_IF = ('') ENCODING = 'UTF8')\n"
            "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\n"
            "FORCE = TRUE;\n"

            "MERGE INTO SAMRA.RAW_DATA.WARNING_STATUS AS T\n"
            "USING SAMRA.RAW_DATA.WARNING_STATUS_TEMP AS S\n"
            "ON T.REG_ID = S.REG_ID \n"
            "AND T.TM_FC = S.TM_FC \n"
            "AND T.WRN = S.WRN \n"
            "AND T.CMD = S.CMD\n"
            "WHEN NOT MATCHED THEN\n"
            "    INSERT (REG_UP, REG_UP_KO, REG_ID, REG_KO, TM_FC, TM_EF, WRN, LVL, "
            "CMD, ED_TM, PROCESSED_AT)\n"
            "    VALUES (S.REG_UP, S.REG_UP_KO, S.REG_ID, S.REG_KO, S.TM_FC, S.TM_EF, "
            "S.WRN, S.LVL, S.CMD, S.ED_TM, S.PROCESSED_AT);"
        ),
        trigger_rule="all_success",
    )

    load_img_snowflake = SQLExecuteQueryOperator(
        task_id="load_img_snowflake",
        conn_id="snowflake_conn_id",
        sql=(
            "COPY INTO SAMRA.RAW_DATA.WARNING_IMG\n"
            "FROM @SAMRA.PUBLIC.WWARN_STAGE/"
            "{{ ti.xcom_pull(task_ids='load_img_meta_to_s3') }}\n"
            "FILE_FORMAT = (TYPE = 'CSV' PARSE_HEADER = TRUE "
            "NULL_IF = ('') ENCODING = 'UTF8')\n"
            "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\n"
            "FORCE = TRUE;"
        ),
        trigger_rule="all_success",
    )

    # =================================================================
    # [DAG 흐름 연결]
    # =================================================================

    text_data = extract_text()
    transformed_text = transform_text(text_data)
    text_s3_key = load_text_to_s3(transformed_text, logical_date="{{ logical_date }}")
    text_s3_key >> load_text_snowflake

    img_meta = process_image(logical_date="{{ logical_date }}")
    img_meta_s3_key = load_img_meta_to_s3(img_meta, logical_date="{{ logical_date }}")
    img_meta_s3_key >> load_img_snowflake


kma_warning_pipeline()