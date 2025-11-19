from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os

# ====== 설정 ======
BASE_URL = "https://www.mois.go.kr"
LIST_URL = "https://www.mois.go.kr/frt/bbs/type001/commonSelectBoardList.do?bbsId=BBSMSTR_000000000336"
DOWNLOAD_DIR = "/tmp"
TEXT_OUTPUT_PATH = "/tmp/latest_report.txt"

SLACK_WEBHOOK_URL = ""   # 👉 여기에 본인 Webhook URL 입력

# ====== DAG 기본 설정 ======
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='mois_report_slack',
    default_args=default_args,
    schedule='0 7 * * *',   # 매일 오후 18시 1분
    catchup=False
)

# ==================================
# 1) 최신 보고서 링크 수집
# ==================================
def crawl_latest_report(**kwargs):
    response = requests.get(LIST_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    first_row = soup.select_one("table tbody tr:nth-of-type(1) a")
    if not first_row:
        raise Exception("게시글 링크를 찾지 못했습니다!")

    file_name = soup.find('div', class_='wrap').text.strip()
    detail_relative_url = first_row["href"]

    detail_url = BASE_URL + detail_relative_url
    print("최신 보고서 URL :",detail_url)
    print("보고서 :",file_name)

    kwargs['ti'].xcom_push(key='detail_url', value=detail_url)
    kwargs['ti'].xcom_push(key='file_name', value=file_name)

# ==================================
# 2) PDF 다운로드
# ==================================
def download_pdf(**kwargs):
    ti = kwargs['ti']
    detail_url = ti.xcom_pull(key='detail_url', task_ids='crawl_latest_report')
    file_name = ti.xcom_pull(key='file_name', task_ids='crawl_latest_report')

    response = requests.get(detail_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    file_list_div = soup.find('div', class_='fileList')
    download_link_tag = file_list_div.find('a')

    relative_path = download_link_tag['href']
    full_download_url = urljoin(BASE_URL, relative_path)
    modified_download_url = full_download_url.replace("fileSn=0", "fileSn=1")

    download_path = os.path.join(DOWNLOAD_DIR, file_name)

    with requests.get(modified_download_url, stream=True) as r:
        r.raise_for_status()
        with open(download_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
    print("다운로드 완료")
    ti.xcom_push(key='download_path', value=download_path)

# ==================================
# 3) PDF 텍스트 추출
# ==================================
def extract_pdf_text(**kwargs):
    import PyPDF2

    ti = kwargs['ti']
    download_path = ti.xcom_pull(key='download_path', task_ids='download_pdf')

    text = ""
    with open(download_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        for page in reader.pages:
            text += page.extract_text()

    os.remove(download_path)

    # 텍스트 파일 저장
    with open(TEXT_OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(text)
    #print("텍스트 추출  완료\n", text[0:500])
    print("보고서 업로드 기준 :", text[34:52])
    print("기상 현황 및 전망 :", text[66:1000])
    send_text = "보고서 작성일 :" + text[34:52] + '\n' + text
    #print("보고서 :",send_text[0:1000])
    ti.xcom_push(key='raw_text', value=text)

# ==================================
# 4) LangChain 기반 AI 요약 생성
# ==================================
def run_ai_agent(**kwargs):
    import requests
    from gradio_client import Client
    client = Client("amd/gpt-oss-120b-chatbot")
    ti = kwargs['ti']
    text = ti.xcom_pull(task_ids="extract_pdf_text", key="raw_text")
    query = f"""다음은 일일상황보고서 내용이다.
                {text}
                
                위 내용에서 기상 현황과 기상 전망에 대해 불렛 형태로 알려줘"""
    output = client.predict(query)
    ti.xcom_push(key="ai_summary", value=output)


# ==================================
# 5) Slack 메시지 보내기
# ==================================
def send_slack(**kwargs):
    import requests

    ti = kwargs['ti']
    summary = ti.xcom_pull(task_ids='run_ai_agent', key='ai_summary')

    payload = {
        "text": f"📌 *오늘의 안전관리상황 요약*\n```{summary}```"
    }

    requests.post(SLACK_WEBHOOK_URL, json=payload)

# ==================================
# Operators
# ==================================
t1 = PythonOperator(task_id='crawl_latest_report', python_callable=crawl_latest_report, dag=dag)
t2 = PythonOperator(task_id='download_pdf', python_callable=download_pdf, dag=dag)
t3 = PythonOperator(task_id='extract_pdf_text', python_callable=extract_pdf_text, dag=dag)
t4 = PythonOperator(task_id='run_ai_agent', python_callable=run_ai_agent, dag=dag)
t5 = PythonOperator(task_id='send_slack', python_callable=send_slack, dag=dag)

# Task 흐름
t1 >> t2 >> t3 >> t4 >> t5
