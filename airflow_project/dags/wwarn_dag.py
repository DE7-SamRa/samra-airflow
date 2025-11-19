# Airflow DAG: KMA Warning Status Pipeline
# 매 정각마다 기상청 특보현황 데이터를 크롤링, 전처리, S3에 적재

import logging
import os
import re
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# ============================================================================
# 기본 설정
# ============================================================================

logger = logging.getLogger(__name__)

# 기상청 API 설정
KMA_SERVICE_KEY = "RReIhJQBRsuXiISUASbLPg"
WRN_NOW_DATA_URL = "https://apihub.kma.go.kr/api/typ01/url/wrn_now_data.php"

# AWS S3 설정
S3_BUCKET = "samra-bucket"
S3_PREFIX = "kma-warning-data"
AWS_CONN_ID = "aws_default"  # Airflow Connection ID

# 로컬 임시 폴더
LOCAL_TEMP_DIR = tempfile.mkdtemp(prefix="airflow_kma_")

# ============================================================================
# DAG 기본 설정
# ============================================================================

default_args = {
    "owner": "정준석",
    "depends_on_past": False,
    "email": ["joonseok.chung@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================================
# Task Functions
# ============================================================================


def parse_kma_format_response(response_text: str) -> List[Dict[str, Any]]:
    """
    기상청 특수 형식 응답 파싱
    10개 컬럼: REG_UP, REG_UP_KO, REG_ID, REG_KO, TM_FC, TM_EF, WRN, LVL, CMD, ED_TM
    """
    logger.info("응답 파싱 시작...")
    items = []
    lines = response_text.strip().split('\n')
    headers = []
    data_started = False

    for _, line in enumerate(lines):
        line_stripped = line.strip()

        # 빈 줄 스킵
        if not line_stripped:
            continue

        # 헤더 라인 감지
        if not data_started and line_stripped.startswith('#'):
            if "REG_UP" in line_stripped and "TM_FC" in line_stripped:
                header_match = re.findall(
                    r'\bREG_UP\b|\bREG_UP_KO\b|\bREG_ID\b|\bREG_KO\b|'
                    r'\bTM_FC\b|\bTM_EF\b|\bWRN\b|\bLVL\b|\bCMD\b|\bED_TM\b',
                    line_stripped
                )

                if header_match:
                    headers = header_match
                    logger.info(
                        f"✓ 헤더 파싱 성공: {len(headers)}개 컬럼 - {headers}"
                    )
            continue

        # 데이터 라인 처리
        if not line_stripped.startswith('#') and headers:
            data_started = True
            values = [v.strip() for v in line_stripped.split(',')]

            if len(values) >= len(headers):
                item_dict = {
                    header: value
                    for header, value in zip(
                        headers, values[:len(headers)], strict=True
                    )
                }
                items.append(item_dict)
            elif len(values) == len(headers) - 1:
                item_dict = {
                    header: value
                    for header, value in zip(headers[:-1], values, strict=True)
                }
                item_dict[headers[-1]] = ""
                items.append(item_dict)

    logger.info(f"✓ 파싱 완료: {len(items)}건")
    return items


@task(task_id="fetch_and_preprocess")
def fetch_and_preprocess():
    """
    Task 1: API에서 데이터 크롤링 및 전처리
    """
    logger.info("=" * 80)
    logger.info("기상청 특보현황 조회 (발표시간 기준)")
    logger.info("=" * 80)

    # 기준시각 설정
    now = datetime.now()
    tm = now.strftime("%Y%m%d%H%M")

    try:
        # API 요청
        params = {
            "fe": "f",  # 발표시간 기준
            "tm": tm,
            "help": 1,
            "authKey": KMA_SERVICE_KEY,
        }

        logger.info(f"요청: {WRN_NOW_DATA_URL}")
        logger.info(f"파라미터: fe=f, tm={tm}")
        logger.info("[API 요청 중...]")

        response = requests.get(
            WRN_NOW_DATA_URL, params=params, timeout=30
        )
        response.raise_for_status()
        response_text = response.text.strip()

        logger.info(f"✓ 응답 수신: {len(response_text)} bytes")

        if not response_text:
            logger.warning("API 응답이 비어있습니다.")
            return None

        # 응답 파싱
        items = parse_kma_format_response(response_text)
        logger.info(f"✓ 조회 성공: {len(items)}건")

        # 데이터 전처리
        logger.info("=" * 80)
        logger.info("데이터 전처리")
        logger.info("=" * 80)

        if not items:
            logger.warning("데이터가 없습니다.")
            return None

        df = pd.DataFrame(items)
        logger.info(f"입력: {len(df)}건, {len(df.columns)}개 컬럼")

        # 필요한 컬럼만 선택
        required_cols = [
            "REG_UP", "REG_UP_KO", "REG_ID", "REG_KO",
            "TM_FC", "TM_EF", "WRN", "LVL", "CMD", "ED_TM"
        ]
        available_cols = [col for col in required_cols if col in df.columns]
        df = df[available_cols]

        # 공백 제거
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()

        # 날짜 형식 변환
        for date_col in ["TM_FC", "TM_EF"]:
            if date_col in df.columns:
                try:
                    df[date_col] = pd.to_datetime(
                        df[date_col].astype(str),
                        format="%Y%m%d%H%M",
                        errors="coerce"
                    )
                    logger.info(f"✓ {date_col} 날짜 변환")
                except Exception as e:
                    logger.error(f"✗ {date_col} 변환 실패: {str(e)}")

        # 처리 시간 추가
        df['processed_at'] = datetime.now()
        logger.info(f"✓ 전처리 완료: {len(df)}건")

        # 로컬 임시 디렉토리 생성
        os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)

        # CSV 저장
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        local_file = os.path.join(
            LOCAL_TEMP_DIR, f"warning_status_{timestamp}.csv"
        )
        df.to_csv(local_file, index=False, encoding='utf-8-sig')

        logger.info("=" * 80)
        logger.info("CSV 저장")
        logger.info("=" * 80)
        logger.info(f"✅ 저장 완료: {local_file}")
        logger.info(f"   데이터: {len(df)}건")
        logger.info(f"   컬럼: {len(df.columns)}개")

        return {"local_file": local_file, "record_count": len(df)}

    except requests.exceptions.Timeout:
        logger.error("❌ 타임아웃 (30초 초과)")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API 오류: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ 오류: {str(e)}")
        raise


@task(task_id="upload_to_s3")
def upload_to_s3(fetch_result: Dict[str, Any]):
    """
    Task 2: S3에 파일 업로드
    """
    logger.info("=" * 80)
    logger.info("S3 업로드 시작")
    logger.info("=" * 80)

    if not fetch_result:
        logger.warning("전처리된 데이터가 없습니다.")
        return None

    local_file = fetch_result.get("local_file")
    record_count = fetch_result.get("record_count")

    if not local_file or not os.path.exists(local_file):
        logger.error(f"로컬 파일을 찾을 수 없습니다: {local_file}")
        raise FileNotFoundError(f"File not found: {local_file}")

    try:
        import boto3
        from botocore.exceptions import ClientError

        # S3 클라이언트 생성
        s3_client = boto3.client('s3')

        # S3 키 생성 - raw_data/wwarn_data로 저장
        s3_key = "raw_data/wwarn_data"

        logger.info(f"파일: {local_file}")
        logger.info(f"버킷: {S3_BUCKET}")
        logger.info(f"키: {s3_key}")

        # S3에 업로드
        s3_client.upload_file(local_file, S3_BUCKET, s3_key)

        logger.info("✅ S3 업로드 성공")
        logger.info(f"   S3 경로: s3://{S3_BUCKET}/{s3_key}")
        logger.info(f"   데이터 건수: {record_count}건")

        # 업로드 완료 후 로컬 파일 삭제
        try:
            os.remove(local_file)
            logger.info(f"✓ 로컬 파일 삭제: {local_file}")
        except Exception as e:
            logger.warning(f"로컬 파일 삭제 실패: {str(e)}")

        return f"s3://{S3_BUCKET}/{s3_key}"

    except ClientError as e:
        logger.error(f"❌ S3 오류: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ 오류: {str(e)}")
        raise


# ============================================================================
# DAG 정의 (TaskFlow API 사용)
# ============================================================================

@dag(
    dag_id="kma_warning_pipeline",
    description="매 정각마다 기상청 특보현황 데이터 크롤링 및 S3 적재",
    default_args=default_args,
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 15),
    tags=["kma", "weather", "data-engineering"],
    catchup=False,
    max_active_runs=1,
)
def kma_warning_pipeline():
    """기상청 특보 데이터 파이프라인"""
    fetch_result = fetch_and_preprocess()
    upload_to_s3(fetch_result)


# DAG 인스턴스 생성
dag_instance = kma_warning_pipeline()
