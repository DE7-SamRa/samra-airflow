# dags/load_air_quality_to_snowflake.py
import time
from datetime import datetime, timedelta

import requests
from airflow.models.variable import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

KR_ISO_CODES = {
    '서울': 'KR-11',
    '부산': 'KR-26',
    '대구': 'KR-27',
    '인천': 'KR-28',
    '광주': 'KR-29',
    '대전': 'KR-30',
    '울산': 'KR-31',
    '세종': 'KR-50',
    '경기': 'KR-41',
    '강원': 'KR-42',
    '충북': 'KR-43',
    '충남': 'KR-44',
    '전북': 'KR-45',
    '전남': 'KR-46',
    '경북': 'KR-47',
    '경남': 'KR-48',
    '제주': 'KR-49'
}

def extract_air_quality_data(**context):
    """Task 1: API에서 대기질 데이터 추출"""

    print("=" * 70)
    print("TASK 1: API 데이터 추출")
    print("=" * 70)

    # API 설정
    base_url = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"
    service_key = Variable.get("AIR_sido_api_key")  # Air Quality API Key

    all_data = []
    num_of_rows = 1000
    page_no = 1
    max_pages = 1000

    params = {
        'serviceKey': service_key,
        'returnType': 'json',
        'numOfRows': num_of_rows,
        'pageNo': 1,
        'sidoName': '전국',
        'ver': '1.0'
    }

    print("데이터 수집 시작...")

    while page_no <= max_pages:
        try:
            params['pageNo'] = page_no

            response = requests.get(base_url, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()

            # API 오류 체크
            result_code = data['response']['header']['resultCode']
            if result_code != '00':
                print(f"  X 페이지 {page_no}: API 오류 코드 {result_code}")
                break

            # 데이터 추출
            items = data['response']['body']['items']

            # 빈 응답 체크
            if not items or (isinstance(items, list) and len(items) == 0):
                print(f"  페이지 {page_no}: 데이터 없음 → 수집 종료")
                break

            if isinstance(items, dict):
                items = [items]

            all_data.extend(items)
            print(f"  페이지 {page_no}: {len(items)}개 수집 (누적: {len(all_data)}개)")

            # 마지막 페이지 감지
            if len(items) < num_of_rows:
                print(f"  페이지 {page_no}: 마지막 페이지 도달")
                break

            page_no += 1
            time.sleep(1)

        except Exception as e:
            print(f"  X 페이지 {page_no}: {e}")
            break

    if len(all_data) == 0:
        raise Exception("X 데이터 추출 실패 또는 없음")
    elif len(all_data) <= 400:
        print("! 경고: 추출된 데이터가 너무 적습니다. 데이터 누락 가능성 있음")
    print(f"\nV 총 {len(all_data)}개 데이터 추출 완료")
    print("=" * 70)

    # XCom으로 다음 task에 데이터 전달
    return all_data


def load_to_snowflake(**context):
    """Task 2: 추출한 데이터를 Snowflake에 적재"""

    print("=" * 70)
    print("TASK 2: Snowflake 데이터 적재")
    print("=" * 70)

    # 이전 task에서 데이터 가져오기
    task_instance = context['task_instance']
    all_data = task_instance.xcom_pull(task_ids='extract')

    if not all_data:
        print("XXX 추출된 데이터가 없습니다. 종료합니다.")
        return

    print(f"VVV {len(all_data)}개 데이터 로드됨")

    # 헬퍼 함수
    def safe_value(value):
        """'-' 또는 빈 값을 None으로 변환"""
        if value in (None, '', '-', 'null', '통신장애'):
            return None
        return value

    # Snowflake Hook 생성
    hook = SnowflakeHook(snowflake_conn_id='snowflake_raw_data')

    success = 0
    failed = 0

    print("\n데이터 적재 시작...")

    for item in all_data:
        try:
            station_name = safe_value(item.get('stationName'))
            sido_name = safe_value(item.get('sidoName'))
            data_time = safe_value(item.get('dataTime'))

            khai_value = safe_value(item.get('khaiValue'))
            khai_grade = safe_value(item.get('khaiGrade'))

            pm10_value = safe_value(item.get('pm10Value'))
            pm10_grade = safe_value(item.get('pm10Grade'))
            pm10_flag = safe_value(item.get('pm10Flag'))

            pm25_value = safe_value(item.get('pm25Value'))
            pm25_grade = safe_value(item.get('pm25Grade'))
            pm25_flag = safe_value(item.get('pm25Flag'))

            o3_value = safe_value(item.get('o3Value'))
            o3_grade = safe_value(item.get('o3Grade'))
            o3_flag = safe_value(item.get('o3Flag'))

            no2_value = safe_value(item.get('no2Value'))
            no2_grade = safe_value(item.get('no2Grade'))
            no2_flag = safe_value(item.get('no2Flag'))

            co_value = safe_value(item.get('coValue'))
            co_grade = safe_value(item.get('coGrade'))
            co_flag = safe_value(item.get('coFlag'))

            so2_value = safe_value(item.get('so2Value'))
            so2_grade = safe_value(item.get('so2Grade'))
            so2_flag = safe_value(item.get('so2Flag'))

            # 데이터 확인
            if station_name is None or sido_name is None:
                print(f"  !!! {item.get('stationName')} 건너뜀: 필수 데이터 없음 !!!")
                continue


# 1)기존 데이터 삭제. 트리거할 때 중복이 생길 수 있어서 중복방지용으로 사용
            hook.run("""
                DELETE FROM samra.raw_data.air_sido_t
                WHERE STATION_NAME = %s AND DATA_TIME = %s
            """, parameters=(
                station_name,
                data_time
            ))


            # 2) 새 데이터 삽입
            hook.run("""
                INSERT INTO samra.raw_data.air_sido_t (
                    STATION_NAME, SIDO_NAME, DATA_TIME,
                    KHAI_VALUE, KHAI_GRADE,
                    PM10_VALUE, PM10_GRADE, PM10_FLAG,
                    PM25_VALUE, PM25_GRADE, PM25_FLAG,
                    O3_VALUE, O3_GRADE, O3_FLAG,
                    NO2_VALUE, NO2_GRADE, NO2_FLAG,
                    CO_VALUE, CO_GRADE, CO_FLAG,
                    SO2_VALUE, SO2_GRADE, SO2_FLAG,
                    CREATED_AT, ISO_CODE
                ) VALUES (
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    CURRENT_TIMESTAMP(),
                    %s
                )
            """, parameters=(
                station_name,
                sido_name,
                data_time,

                khai_value,
                khai_grade,

                pm10_value,
                pm10_grade,
                pm10_flag,

                pm25_value,
                pm25_grade,
                pm25_flag,

                o3_value,
                o3_grade,
                o3_flag,

                no2_value,
                no2_grade,
                no2_flag,

                co_value,
                co_grade,
                co_flag,

                so2_value,
                so2_grade,
                so2_flag,

                KR_ISO_CODES.get(item.get('sidoName'), None)
            ))

            success += 1

            if success % 100 == 0:
                print(f"  진행: {success}개 완료...")

        except Exception as e:
            failed += 1
            station = item.get('stationName', 'Unknown')
            print(f"  X {station} 실패: {e}")

    # 결과 확인
    print("\n" + "=" * 70)
    print("결과 확인")
    print("=" * 70)

    print(f"VV 성공: {success}개")
    print(f"XX 실패: {failed}개")

    # 총 레코드 수
    result = hook.get_records("SELECT COUNT(*) FROM samra.raw_data.air_sido_t")
    print(f"\n총 레코드 수: {result[0][0]}")

    # 최신 데이터 샘플
    sample = hook.get_records("""
        SELECT
            STATION_NAME, SIDO_NAME, DATA_TIME,
            PM10_VALUE, PM10_GRADE,
            PM25_VALUE, PM25_GRADE
        FROM samra.raw_data.air_sido_t
        ORDER BY DATA_TIME DESC
        LIMIT 5
    """)

    print("\n최신 데이터 샘플:")
    for row in sample:
        print(f"  - {row[0]} ({row[1]}) | {row[2]}")
        print(f"PM10: {row[3]}({row[4]}) (PM2.5: {row[5]}({row[6]})")
    print("\n" + "=" * 70)
    print("VV 데이터 적재 완료!")
    print("=" * 70)


def validate_data(**context):
    """Task 3: 데이터 검증 - 모든 지역에 데이터가 있는지 확인"""

    print("=" * 70)
    print("TASK 3: 데이터 검증")
    print("=" * 70)

    # 기대하는 지역 목록 (17개)
    expected_regions = [
        '서울', '부산', '대구', '인천', '광주', '대전', '울산', '세종',
        '경기', '강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주'
    ]

    hook = SnowflakeHook(snowflake_conn_id='snowflake_raw_data')

    print(f"✓ 기대 지역 수: {len(expected_regions)}개")
    print(f"✓ 기대 지역: {', '.join(expected_regions)}\n")

    # 1. 지역별 데이터 개수 조회
    print("지역별 데이터 확인 중...")
    result = hook.get_records("""
        SELECT
            SIDO_NAME,
            COUNT(*) as cnt
        FROM samra.raw_data.air_sido_t
        WHERE DATA_TIME >= DATEADD(HOUR, -2, CURRENT_TIMESTAMP())  -- 최근 2시간 데이터
        GROUP BY SIDO_NAME
        ORDER BY SIDO_NAME
    """)

    # 결과를 dict로 변환
    actual_regions = {row[0]: row[1] for row in result}

    print("\n실제 데이터가 있는 지역:")
    for sido, count in sorted(actual_regions.items()):
        print(f"  - {sido}: {count}개")

    # 2. 검증
    print("\n" + "=" * 70)
    print("검증 결과")
    print("=" * 70)

    missing_regions = []
    present_regions = []

    for region in expected_regions:
        count = actual_regions.get(region, 0)

        if count >= 1:
            present_regions.append(region)
            print(f" VVV {region}: {count}개 (정상)")
        else:
            missing_regions.append(region)
            print(f" XXX {region}: 0개 (누락!)")

    # 3. 결과 요약
    print("\n" + "=" * 70)
    print("요약")
    print("=" * 70)
    print(f"VVV 데이터 있는 지역: {len(present_regions)}/{len(expected_regions)}개")
    print(f"XXX 데이터 없는 지역: {len(missing_regions)}/{len(expected_regions)}개")

    if missing_regions:
        print(f"\n!!! 누락된 지역: {', '.join(missing_regions)}")

    # 4. 전체 데이터 통계
    total = hook.get_records("""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT STATION_NAME) as stations,
            MIN(DATA_TIME) as min_time,
            MAX(DATA_TIME) as max_time
        FROM samra.raw_data.air_sido_t
        WHERE DATA_TIME >= DATEADD(HOUR, -2, CURRENT_TIMESTAMP())
    """)

    print("\n전체 통계 (최근 2시간):")
    print(f"  총 레코드: {total[0][0]}개")
    print(f"  측정소 수: {total[0][1]}개")
    print(f"  최소 시간: {total[0][2]}")
    print(f"  최대 시간: {total[0][3]}")

    # 5. 검증 성공/실패 판단
    print("\n" + "=" * 70)

    if len(missing_regions) == 0:
        print(" VVV 검증 성공! 모든 지역의 데이터가 존재합니다.")
        print("=" * 70)
        return True
    elif len(missing_regions) <= 2:
        # 2개 이하 누락은 경고만
        print(f"!!! 검증 경고: {len(missing_regions)}개 지역 데이터 누락")
        print("=" * 70)
        return True
    else:
        # 3개 이상 누락은 실패
        print(f"XXX 검증 실패! {len(missing_regions)}개 지역 데이터 누락")
        print("=" * 70)
        raise ValueError(
            f"검증 실패: {len(missing_regions)}개 누락 {','.join(missing_regions)}")


# DAG 정의
with DAG(
    'air_quality_etl',
    description='대기질 데이터 ETL: Extract → Load',
    schedule='0 * * * *',  # 매시간
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['air-quality', 'etl', 'snowflake_raw_data'],
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:

    # Task 1: Extract
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_air_quality_data
    )

    # Task 2: Load
    load = PythonOperator(
        task_id='load',
        python_callable=load_to_snowflake
    )

    # Task 3: Validate (선택 사항)
    # 지역별로 데이터가 들어왔는지 확인
    validate = PythonOperator(
        task_id='validate',
        python_callable=validate_data
    )

    # Task 의존성 설정: extract → load → validate
    extract >> load >> validate
