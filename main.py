import os
from etl.extract.extract_from_api import extract
from etl.transform.transform import trasform
from etl.load.load import load
from dotenv import load_dotenv
from loguru import logger

logger.add("logs/main.log")

load_dotenv()

API_KEY = os.getenv("API_KEY")
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
bucket = os.getenv("bucket")

if __name__ == "__main__":
    raw_data = extract(API_KEY)
    input_file = trasform(raw_data)
    load(input_file, bucket, aws_access_key_id, aws_secret_access_key)
    
