import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EXCLUDED_DIRS = {"__pycache__"}
EXCLUDED_FILES = {"load_to_minio.py"}

def load_to_minio():
    try:
        # Load config
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', '.env')
        load_dotenv(config_path)
        
        endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
        access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        bucket = 'data-lake'

        s3 = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'
        )

        # Create bucket if not exists
        try:
            s3.create_bucket(Bucket=bucket)
            logger.info(f"Created bucket {bucket}")
        except ClientError as e:
            if e.response['Error']['Code'] not in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
                raise
            else:
                logger.info(f"Bucket {bucket} already exists")

        storage_dir = os.path.abspath(os.path.dirname(__file__))
        if not os.path.exists(storage_dir):
            logger.warning("Storage directory does not exist.")
            return

        uploaded_files = 0
        for root, dirs, files in os.walk(storage_dir):
            dirs[:] = [directory for directory in dirs if directory not in EXCLUDED_DIRS]

            for file in files:
                if file in EXCLUDED_FILES:
                    continue

                local_path = os.path.join(root, file)
                s3_path = os.path.relpath(local_path, storage_dir).replace('\\', '/')
                s3.upload_file(local_path, bucket, s3_path)
                uploaded_files += 1
                logger.info(f"Uploaded {s3_path}")

        logger.info(f"Load to MinIO completed successfully. Uploaded {uploaded_files} files.")

    except NoCredentialsError:
        logger.error("Credentials not available.")
        raise
    except Exception as e:
        logger.error(f"Error in loading to MinIO: {str(e)}")
        raise

if __name__ == "__main__":
    load_to_minio()
