# Initial-Setup
본 페이지에서는 설치 과정을 설명함

## EC2 인스턴스
- OS : Ubuntu
- Amazon Machine Image(AMI) : Ubuntu Server 24.04 LTS
- Instance : m7i-flex.large (프리 티어 기준 사용 가능한 고성능 인스턴스)
- Storage : 10GiB
- Inbound Rules : 0.0.0.0:8080 (Airflow 사용을 위한 8080 포트 개방)

## Docker & Airflow 설치
두 공식 홈페이지의 설명에 따라 install_airflow.sh 작성
- Docker : https://docs.docker.com/engine/install/ubuntu/#upgrade-docker-engine-1
- Airflow : https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
sh 파일 실행
```
sh install_airflow.sh
```
./airflow_project/.env 파일에 airflow 아이디와 비밀번호 추가
```
_AIRFLOW_WWW_USER_USERNAME=your_id
_AIRFLOW_WWW_USER_PASSWORD=your_password
```
./airflow/docker-compose.yml 파일에 examples를 false로 설정
```
AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
```
이후 docker compose 추가 명령어를 통해 airflow 가동
```
sudo docker compose run airflow-cli airflow config list
sudo docker compose up airflow-init
sudo docker compose up -d
```
