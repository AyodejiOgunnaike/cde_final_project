import boto3
import os


def upload_parquet_to_s3(file_path, bucket_name, object_name=None):
    """
    Upload a local Parquet file to an S3 bucket.

    Args:
        file_path (str): Full path to the Parquet file to be uploaded.
        bucket_name (str): Name of the target S3 bucket.
        object_name (str, optional): Key for the uploaded file in S3.
                                     Defaults to the file's base name.

    Returns:
        bool: True if upload succeeded, False otherwise.
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return False

    if not file_path.endswith(".parquet"):
        print(f"Invalid file type: {file_path}. Only Parquet files are supported.")
        return False

    if object_name is None:
        object_name = os.path.basename(file_path)

# Upload the file
    try:
        s3_client = boto3.client("s3")
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"File uploaded successfully to S3 bucket '{bucket_name}' as '{object_name}'")
        return True
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return False
    

# Example Usage
if __name__ == "__main__":
    LOCAL_PARQUET_FILE = r'C:/Users/Administrator/Desktop/countries_data.parquet'  # Replace with your file path
    S3_BUCKET_NAME = "ayodeji-terraform-bucket"                 # Replace with your bucket name
    S3_OBJECT_NAME = "countries_data.parquet"              # Optional, can be None

    upload_parquet_to_s3(LOCAL_PARQUET_FILE, S3_BUCKET_NAME, S3_OBJECT_NAME)








