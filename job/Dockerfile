# Base image: Airflow official (Composer chạy Python 3.11)
FROM apache/airflow:2.10.2-python3.11

# Cài đặt system dependencies cần cho matplotlib (font, freetype…)
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libfreetype6-dev \
    libpng-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt /requirements.txt

# Cài Python libs
RUN pip install --no-cache-dir -r /requirements.txt

# Đảm bảo quay lại user airflow
USER airflow
