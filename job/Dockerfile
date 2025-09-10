# Base image Python
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements trước (để tận dụng cache docker)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào container
COPY . .

# Default command (có thể đổi khi chạy job khác)
CMD ["python", "Metrics_Sales.py"]
