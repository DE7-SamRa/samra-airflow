import logging

import pendulum
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import DAG, task


def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn_id')
    return hook.get_conn().cursor()


@task
def extract_from_s3_to_snowflake(snowflake_cur, date):
    # snowflake에 적재할 테이블 copy to 실행
    logging.info(
        f'ASOS Extract Start: s3://samra-bucket/raw_data/ASOS/ASOS_{date}.parquet')
    try:
        snowflake_cur.execute("BEGIN;")
        snowflake_cur.execute("USE DATABASE SAMRA;") # 못가져오면 에러 띄우기
        create_or_replace_query = f"""
        CREATE OR REPLACE TABLE SAMRA.RAW_DATA.ASOS_TEMPORAL_{date}(
            TM VARCHAR,
            STNID VARCHAR,
            STNNM VARCHAR,
            TA VARCHAR,
            RN VARCHAR,
            WS VARCHAR,
            WD VARCHAR,
            HM VARCHAR,
            DSNW VARCHAR
        );
        """
        snowflake_cur.execute(create_or_replace_query)
        stage_creation_query = """
        CREATE OR REPLACE STAGE ASOS_stage
        STORAGE_INTEGRATION = S3_SAMRA
        URL = 's3://samra-bucket/raw_data/ASOS/'
        FILE_FORMAT = (TYPE = PARQUET);
        """
        snowflake_cur.execute(stage_creation_query)
        copy_into_query = f"""
        COPY INTO SAMRA.RAW_DATA.ASOS_TEMPORAL_{date}
        FROM @ASOS_stage/ASOS_{date}.parquet
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """
        snowflake_cur.execute(copy_into_query)
        snowflake_cur.execute("COMMIT;")
    except Exception as error:
        logging.error(f'Error during Snowflake copy into: {error}')
        snowflake_cur.execute("ROLLBACK;")
        raise


@task
def transform_raw_data_and_load(snowflake_cur, date):
    logging.info('ASOS Transform and Load Start')
    try:
        # 1. temporal 테이블을 통합 테이블에 적재
        snowflake_cur.execute("BEGIN;")
        snowflake_cur.execute("USE DATABASE SAMRA;")
        create_table_if_not_exists = """
            CREATE TABLE IF NOT EXISTS SAMRA.RAW_DATA.ASOS_WEATHER (
                "시간" VARCHAR,
                "지역코드" INT,
                "지역명" VARCHAR,
                "온도" FLOAT,
                "풍속" FLOAT,
                "풍향" INT,
                "강수량" FLOAT,
                "습도(%)" INT,
                "적설" FLOAT
            );
            """
        snowflake_cur.execute(create_table_if_not_exists)
        insert_temporal_data_into = f"""
            INSERT INTO SAMRA.RAW_DATA.ASOS_WEATHER
            ("시간","지역코드","지역명","온도","풍속","풍향","강수량","습도(%)","적설")
            SELECT
                TM::VARCHAR,
                STNID::INT,
                STNNM::VARCHAR,
                NULLIF(TA, '')::FLOAT,
                NULLIF(WS, '')::FLOAT,
                NULLIF(WD, '')::INT,
                NULLIF(RN, '')::FLOAT,
                NULLIF(HM, '')::INT,
                NULLIF(DSNW, '')::FLOAT
            FROM SAMRA.RAW_DATA.ASOS_TEMPORAL_{date};""" #noqa : S608
        snowflake_cur.execute(insert_temporal_data_into)
        snowflake_cur.execute(
            f"DROP TABLE IF EXISTS SAMRA.RAW_DATA.ASOS_TEMPORAL_{date};")
        snowflake_cur.execute("COMMIT;")
    except Exception as error:
        logging.error(f'Error during Snowflake copy into: {error}')
        snowflake_cur.execute("ROLLBACK;")
        raise


with DAG(
    dag_id='ASOS_ETL',
    schedule='0 13 * * *',
    start_date=pendulum.datetime(2025, 11, 11, 13, 0, tz="Asia/Seoul"),
    max_active_runs=1,
    catchup=True,
) as dag:
    date = "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}"

    # snowflake 연결 설정
    snowflake_cur = get_snowflake_connection()

    # S3에서 Snowflake로 데이터 추출 및 적재
    extract = extract_from_s3_to_snowflake(snowflake_cur, date)

    # Snowflake에서 데이터 변환 및 분석 테이블로 적재
    transform = transform_raw_data_and_load(snowflake_cur, date)
    extract >> transform
