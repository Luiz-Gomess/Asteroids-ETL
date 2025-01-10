from etl.extract.extract_from_api import extract_data_from_api
from etl.transform.transform import trasform

if __name__ == "__main__":
    raw_data = extract_data_from_api()
    processed_data = trasform(raw_data)
    print(processed_data)
    
