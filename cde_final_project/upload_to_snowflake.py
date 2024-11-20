import os
import keyring
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

def upload_parquet_to_snowflake(file_path, stage_name, connection_details):
    """
    Upload a local Parquet file to a Snowflake stage.

    Args:
        file_path (str): Full path to the Parquet file to be uploaded.
        stage_name (str): Name of the Snowflake stage (e.g., '@countries_data_stage').
        connection_details (dict): Snowflake connection details.

    Returns:
        bool: True if upload succeeded, False otherwise.
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return False

    if not file_path.endswith(".parquet"):
        print(f"Invalid file type: {file_path}. Only Parquet files are supported.")
        return False

    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(**connection_details)
        cursor = conn.cursor()

        # Upload the file to the stage
        file_name = os.path.basename(file_path)
        put_query = f"PUT file://{file_path} {stage_name}/{file_name} AUTO_COMPRESS=TRUE"
        cursor.execute(put_query)

        print(f"File '{file_path}' successfully uploaded to Snowflake stage '{stage_name}'.")
        return True

    except ProgrammingError as e:
        print(f"Snowflake error: {e}")
        return False

    except Exception as e:
        print(f"Error uploading file to Snowflake: {e}")
        return False

    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    # Replace these with your actual file and Snowflake details
    LOCAL_PARQUET_FILE = r'C:/Users/Administrator/Desktop/countries_data.parquet'
    SNOWFLAKE_STAGE = "@countries_data_stage"  # Prefix stage name with @

    # Store your Snowflake password securely using keyring
    SERVICE_NAME = "Snowflake"
    USERNAME = "AyodejiOgunnaike"

    # Use `keyring` to retrieve the password
    password = keyring.get_password(SERVICE_NAME, USERNAME)

    if not password:
        print(f"Password not found in keyring for {USERNAME}. Please set it first.")
        exit(1)

    # Snowflake connection details
    SNOWFLAKE_CONNECTION_DETAILS = {
        "user": USERNAME,
        "password": password,
        "account": "hsntpwy-am50061",
        "warehouse": "COMPUTE_WH",
        "database": "COUNTRIES_DATA",
        "schema": "PUBLIC",
        "role": "ACCOUNTADMIN"
    }

    upload_parquet_to_snowflake(LOCAL_PARQUET_FILE, SNOWFLAKE_STAGE, SNOWFLAKE_CONNECTION_DETAILS)
