#!/bin/bash
# Installing Docker
echo "--- Removing conflicting packages ---"
sudo apt remove $(dpkg --get-selections docker.io docker-compose docker-compose-v2 docker-doc podman-docker containerd runc | cut -f1)

echo "--- Add Docker's official GPG key: ---"
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo "--- Add the repository to Apt sources: ---"
sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF

echo "--- Installing docker ---"
sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y

# Installing Airflow
echo "--- Copy Airflow's docker-compose.yaml ---"
mkdir airflow_project
cd airflow_project
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.2/docker-compose.yaml'
mkdir -p ./dags ./logs ./plugins ./config
echo "AIRFLOW_UID=$(id -u)" > .env
#sudo docker compose run airflow-cli airflow config list
#sudo docker compose up airflow-init
#sudo docker compose up -d
