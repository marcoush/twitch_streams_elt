import logging
import requests
import json
import tempfile
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# Twitch API config
CLIENT_ID = "1izmkc14jnnvu9adawkypufgn1gzg6"
CLIENT_SECRET = "tgchzrj9g06kgprgro15lwilwgu51m"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"
STREAMS_URL = "https://api.twitch.tv/helix/streams"

default_args = {
    "owner": "airflow",
}

@dag(
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["twitch", "ingest", "snowflake"]
)
def twitch_api_snowflake_ingest_dag():

    @task()
    def get_app_access_token():
        """Get Twitch app access token via Client Credentials Flow"""
        params = {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
        }
        resp = requests.post(TOKEN_URL, params=params)
        resp.raise_for_status()
        token = resp.json().get("access_token")
        logging.info("Obtained Twitch app access token")
        return token

    @task()
    def fetch_live_streams(token: str):
        """Fetch current live streams and return JSON string"""
        headers = {
            "Client-ID": CLIENT_ID,
            "Authorization": f"Bearer {token}",
        }
        resp = requests.get(STREAMS_URL, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        json_str = json.dumps(data, indent=2)
        logging.info("Fetched Twitch live streams")
        return json_str

    @task()
    def upload_json_file_to_snowflake(json_data: str):
        """Upload JSON file to Snowflake stage and COPY INTO raw table"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"twitch_streams_{timestamp}.json"

        # Write temp file
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            f.write(json_data)
            temp_file_path = f.name

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Stage and table setup
        cursor.execute("USE DATABASE TWITCHERS;")
        cursor.execute("USE SCHEMA YEYE;")
        cursor.execute("CREATE STAGE IF NOT EXISTS TWITCH_STAGE;")
        cursor.execute("CREATE TABLE IF NOT EXISTS TWITCH_STREAMS (RAW_JSON VARIANT, FILE_NAME VARCHAR, LOAD_TIME TIMESTAMP);")

        # Upload JSON file to stage
        cursor.execute(f"""
            PUT file://{temp_file_path} @TWITCH_STAGE/{filename} AUTO_COMPRESS=FALSE;
        """)

        # Copy into raw table
        cursor.execute("""
            COPY INTO TWITCH_STREAMS
            FROM (
                SELECT $1, METADATA$FILENAME, CURRENT_TIMESTAMP()
                FROM @TWITCH_STAGE
            )
            FILE_FORMAT = (TYPE = JSON);
        """)

        conn.commit()
        cursor.close()
        logging.info(f"Uploaded and copied {filename} into TWITCH_STREAMS")

    # TaskFlow dependencies
    token = get_app_access_token()
    streams_json = fetch_live_streams(token)
    upload_json_file_to_snowflake(streams_json)


abc = twitch_api_snowflake_ingest_dag()
# the dag id is what the def function is named, so the variable name doesn't matter.