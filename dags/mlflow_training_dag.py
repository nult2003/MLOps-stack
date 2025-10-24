from airflow.decorators import dag, task
from datetime import datetime
import os

@dag(start_date=datetime(2023, 1, 1), schedule=None, catchup=False, tags=['mlops'])
def mlflow_training_pipeline():
    @task
    def train_and_log_model():
        import mlflow
        import pickle
        
        # Tạo thư mục mlruns nếu nó chưa tồn tại (chỉ cần trong JupyterLab)
        MLRUNS_DIR = os.path.join("..", "mlruns")
        if not os.path.exists(MLRUNS_DIR):
            os.makedirs(MLRUNS_DIR)

        # === KẾT NỐI VỚI MLFLOW SERVER BẰNG TÊN DỊCH VỤ NỘI BỘ ===
        # Đây là cách Airflow (Client) kết nối với MLflow Server trong mạng Docker
        mlflow.set_tracking_uri("http://10.89.0.4:5000") 
        mlflow.set_experiment("Airflow Pipeline")

        # 1. Huấn luyện mô hình giả
        class MockModel:
            def predict(self, x):
                return x * 2
        mock_model = MockModel()

        # 2. Bắt đầu Run và Log
        with mlflow.start_run(run_name="automated_run") as run:
            mlflow.log_param("orchestrator", "Airflow")
            mlflow.log_metric("mock_accuracy", 0.95)

            # Lưu mô hình cục bộ tạm thời (Airflow Worker)
            # Đặt tệp trong thư mục tạm thời, bên trong thư mục mlruns đã được mount
            model_path = os.path.join(MLRUNS_DIR, "airflow_mock_model.pkl")
            with open(model_path, "wb") as f:
                pickle.dump(mock_model, f)

            # Gửi tệp artifact tới MLflow Server
            mlflow.log_artifact(model_path, artifact_path="model")

            print(f"MLflow Run ID: {run.info.run_id}")

    train_and_log_model()

mlflow_training_dag = mlflow_training_pipeline()