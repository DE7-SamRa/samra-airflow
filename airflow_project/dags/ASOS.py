import io
import json
import logging
import os

import pandas as pd
import pendulum
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import DAG, Variable, task


@task
def get_region_ids():
    # 지역 번호 가져오기
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'regions.json')
    with open(CONFIG_PATH, 'r') as json_file:
        regions = json.load(json_file)
    stnIds = [i['id'] for i in regions]
    return stnIds


@task
def extract(startHh, endHh, stnIds, date):

    result = []
    for id in stnIds:
        logging.info(
            f'ASOS Extract Start\nstartDt: {date} startHh: {startHh}\
                  endDt: {date} endHh: {endHh} stnIds: {id}')

        # ASOS API 호출을 위한 필수 인자 설정 및 API 호출
        url = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
        params = {
            # 필수값
            'serviceKey': Variable.get('ASOS_key'),
            'dataCd': 'ASOS',
            'dateCd': 'HR',
            'startDt': date,
            'startHh': startHh,
            'endDt': date,
            'endHh': endHh,
            'stnIds': id,
            # 옵션값
            'numOfRows': '24',
            'dataType': 'JSON',
        }
        response = requests.get(url, params=params, timeout=60)
        result.append(response.json())

    return result


@task
def transform(extracted_data):
    logging.info(f'ASOS Transform Start\nCollected Hours : {len(extracted_data)}')

    # JSON 데이터를 Pandas DataFrame으로 변환
    weather_df = pd.DataFrame()
    for data in extracted_data:
        weather_df = pd.concat([
            weather_df,
            pd.DataFrame(data['response']['body']['items']['item'])],
            ignore_index=True
        )
    return weather_df


@task
def load(weather_df, date):
    logging.info('ASOS Load Start')

    # DataFrame을 Parquet 형식으로 변환
    buffer = io.BytesIO()
    weather_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # S3Hook을 통해 S3에 데이터 적재
    hook = S3Hook(aws_conn_id='s3_key')
    hook.load_bytes(
        bytes_data=buffer.getvalue(),
        key=f'raw_data/ASOS/ASOS_{date}.parquet',
        bucket_name='samra-bucket',
        replace=True
    )
    logging.info(f'ASOS Load Complete: s3://samra-bucket/raw_data/ASOS/ASOS_{date}.parquet')


with DAG(
    dag_id='ASOS',
    schedule='0 12 * * *',  # 매일 12시에 실행
    start_date=pendulum.datetime(2025, 11, 11, 12, 0, tz="Asia/Seoul"),
    catchup=True
) as dag:
    date = "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y%m%d') }}"

    # 지역 번호 리스트 가져오기
    stnIds = get_region_ids()

    # 지역별 어제 날짜 날씨 데이터 추출
    extracted_data = extract(startHh='00', endHh='23',
                             stnIds=stnIds, date=date)

    # 추출된 데이터 병합
    weather_df = transform(extracted_data)

    # 데이터 적재
    load(weather_df, date=date)
