import os
import tempfile
from datetime import datetime, timedelta
from urllib.parse import urljoin

import PyPDF2
import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from bs4 import BeautifulSoup
from gradio_client import Client

# ====== 설정 ======

BASE_URL = "https://www.mois.go.kr"
LIST_URL = ("https://www.mois.go.kr/frt/bbs/type001/"
            "commonSelectBoardList.do?bbsId=BBSMSTR_000000000336")

# 안전한 임시 디렉토리 사용
TMP_DIR = tempfile.gettempdir()
TEXT_OUTPUT_PATH = os.path.join(TMP_DIR, "latest_report.txt")

SLACK_WEBHOOK_URL = ("https://hooks.slack.com/services/T09SZ0BSHEU"
                     "/B09TFUJ97GD/rRx0cPQY0lgOJMshFSp6wx4r")

# ====== DAG 설정 ======
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 17),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mois_report_slack",
    default_args=default_args,
    schedule="0 7 * * *",
    catchup=False,
) as dag:
    # ==================================
    # 1) 최신 보고서 링크 수집
    # ==================================
    def crawl_latest_report(**kwargs):
        response = requests.get(LIST_URL, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        first_row = soup.select_one("table tbody tr:nth-of-type(1) a")
        if not first_row:
            raise ValueError("게시글 링크를 찾지 못했습니다!")

        file_name = soup.find("div", class_="wrap").text.strip()
        detail_url = urljoin(BASE_URL, first_row["href"])

        print("최신 보고서 URL:", detail_url)
        print("보고서:", file_name)

        ti = kwargs["ti"]
        ti.xcom_push(key="detail_url", value=detail_url)
        ti.xcom_push(key="file_name", value=file_name)

    # ==================================
    # 2) PDF 다운로드
    # ==================================
    def download_pdf(**kwargs):
        ti = kwargs["ti"]
        detail_url = ti.xcom_pull(
            key="detail_url",
            task_ids="crawl_latest_report",
        )
        file_name = ti.xcom_pull(
            key="file_name",
            task_ids="crawl_latest_report",
        )

        response = requests.get(detail_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        file_list_div = soup.find("div", class_="fileList")
        download_link_tag = file_list_div.find("a")

        relative_path = download_link_tag["href"]
        full_download_url = urljoin(BASE_URL, relative_path)
        modified_download_url = full_download_url.replace(
            "fileSn=0",
            "fileSn=1",
        )

        download_path = os.path.join(TMP_DIR, file_name)

        with requests.get(
            modified_download_url,
            stream=True,
            timeout=10,
        ) as r:
            r.raise_for_status()
            with open(download_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

        print("PDF 다운로드 완료")
        ti.xcom_push(key="download_path", value=download_path)

    # ==================================
    # 3) PDF 텍스트 추출
    # ==================================
    def extract_pdf_text(**kwargs):

        ti = kwargs["ti"]
        download_path = ti.xcom_pull(
            key="download_path",
            task_ids="download_pdf",
        )

        text = ""
        with open(download_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text += page.extract_text()

        os.remove(download_path)

        with open(TEXT_OUTPUT_PATH, "w", encoding="utf-8") as f:
            f.write(text)

        print("보고서 업로드 기준:", text[34:52])
        print("기상 현황 및 전망:", text[66:150])

        ti.xcom_push(key="raw_text", value=text)

    # ==================================
    # 4) AI 요약 생성
    # ==================================
    def run_ai_agent(**kwargs):

        ti = kwargs["ti"]
        text = ti.xcom_pull(
            key="raw_text",
            task_ids="extract_pdf_text",
        )

        client = Client("amd/gpt-oss-120b-chatbot")

        query = (
            "다음은 일일상황보고서 내용이다.\n"
            f"{text}\n\n"
            "위 내용에서 기상 현황과 기상 전망을 "
            "bullet 형식으로 요약해줘."
        )

        output = client.predict(query)
        ti.xcom_push(key="ai_summary", value=output)

    # ==================================
    # 5) Slack 메시지 보내기
    # ==================================
    def send_slack(**kwargs):
        ti = kwargs["ti"]
        summary = ti.xcom_pull(
            key="ai_summary",
            task_ids="run_ai_agent",
        )

        payload = {"text": (f"📌 *오늘의 안전관리상황 요약*\n```{summary}```")}

        requests.post(
            SLACK_WEBHOOK_URL,
            json=payload,
            timeout=10,
        )

    # Operators
    crawl_latest_report = PythonOperator(
        task_id="crawl_latest_report",
        python_callable=crawl_latest_report,
    )

    download_pdf = PythonOperator(
        task_id="download_pdf",
        python_callable=download_pdf,
    )

    extract_pdf_text = PythonOperator(
        task_id="extract_pdf_text",
        python_callable=extract_pdf_text,
    )

    run_ai_agent = PythonOperator(
        task_id="run_ai_agent",
        python_callable=run_ai_agent,
    )

    send_slack = PythonOperator(
        task_id="send_slack",
        python_callable=send_slack,
    )

    (
        crawl_latest_report
        >> download_pdf
        >> extract_pdf_text
        >> run_ai_agent
        >> send_slack
    )
