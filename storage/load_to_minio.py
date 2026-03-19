import logging
import os
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError, NoCredentialsError
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

EXCLUDED_DIRS = {"__pycache__"}
EXCLUDED_FILES = {"load_to_minio.py"}


def build_endpoint_candidates():
    env_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    candidates = [env_endpoint]

    parsed_endpoint = urlparse(env_endpoint)
    hostname = parsed_endpoint.hostname
    if hostname == 'localhost':
        candidates.append(env_endpoint.replace('localhost', 'minio', 1))
    elif hostname == 'minio':
        candidates.append(env_endpoint.replace('minio', 'localhost', 1))

    unique_candidates = []
    for candidate in candidates:
        if candidate not in unique_candidates:
            unique_candidates.append(candidate)
    return unique_candidates


def build_s3_client(endpoint, access_key, secret_key):
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='us-east-1'
    )

def load_to_minio(required=True):
    try:
        # Load config
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', '.env')
        load_dotenv(config_path)
        
        access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        bucket = 'data-lake'
        last_error = None
        s3 = None
        endpoint = None

        for candidate_endpoint in build_endpoint_candidates():
            try:
                s3 = build_s3_client(candidate_endpoint, access_key, secret_key)
                s3.list_buckets()
                endpoint = candidate_endpoint
                logger.info("Connected to MinIO at %s", endpoint)
                break
            except Exception as exc:
                last_error = exc
                logger.warning("Failed to connect to MinIO at %s: %s", candidate_endpoint, exc)

        if s3 is None or endpoint is None:
            if not required:
                logger.warning(
                    "MinIO is unavailable. Skipping load step because it is optional for local execution."
                )
                return False
            raise last_error

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
        return True

    except NoCredentialsError:
        logger.error("Credentials not available.")
        raise
    except EndpointConnectionError as e:
        if not required:
            logger.warning(
                "MinIO endpoint is unreachable. Skipping load step because it is optional for local execution."
            )
            return False
        logger.error(f"Error in loading to MinIO: {str(e)}")
        raise
    except Exception as e:
        if not required:
            logger.warning(
                "MinIO load failed during optional local execution. Skipping load step. Error: %s",
                str(e),
            )
            return False
        logger.error(f"Error in loading to MinIO: {str(e)}")
        raise

if __name__ == "__main__":
    load_to_minio()
