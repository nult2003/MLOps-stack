# Sử dụng image Airflow tiêu chuẩn làm base
FROM apache/airflow:2.8.2-python3.11

# Copy file requirements và cài đặt
COPY requirements.txt .

# Sử dụng pip để cài đặt các package
RUN pip install --no-cache-dir -r requirements.txt