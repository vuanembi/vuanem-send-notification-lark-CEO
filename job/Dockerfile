FROM apache/airflow:2.10.2-python3.11

# Cài đặt gói hệ thống (chạy bằng root)
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libfreetype6-dev \
    libpng-dev \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt /requirements.txt

# Chuyển sang user airflow để pip install
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
