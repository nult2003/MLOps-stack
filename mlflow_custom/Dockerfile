# Dockerfile trong thư mục mlflow_custom/
FROM ghcr.io/mlflow/mlflow:latest

# Cài đặt driver PostgreSQL
RUN pip install psycopg2-binary

# (Nếu cần thêm các thư viện khác, bạn có thể thêm ở đây)

# Tạo user có cùng UID/GID với Jupyter (jovyan)
RUN addgroup --gid 100 jovyan && adduser --disabled-password --gecos "" --uid 1000 --gid 100 jovyan

# Đảm bảo quyền ghi cho thư mục artifacts
RUN mkdir -p /opt/mlflow/artifacts /home/jovyan/mlruns && chown -R jovyan:jovyan /opt/mlflow /home/jovyan/mlruns


# Chạy dưới user jovyan
USER jovyan
RUN chmod -R 777 /opt/mlflow/artifacts
RUN chmod -R 777 /opt/mlflow
RUN chmod -R 777 /home/jovyan