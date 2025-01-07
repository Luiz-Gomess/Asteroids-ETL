import datetime
import os   
import pandas as pd
import requests
import sys
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from find_key import find_key
from loguru import logger

load_dotenv()

logger.add("logs/extract.log")

def extract_data_from_api():
    try:
        CURRENT_DATE = datetime.date.today()
        START_DATE = CURRENT_DATE - relativedelta(days=7)

        file_path = os.path.join(os.getcwd(), "data", "raw", CURRENT_DATE.strftime("%m"), "raw_data.parquet")
        api_url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={START_DATE}&end_date={CURRENT_DATE}&api_key={os.getenv('API_KEY')}"

        try:
            response = requests.get(api_url)
            logger.info(f"{response.status_code} - Chamada de API bem-sucedida")
        except requests.exceptions.RequestException as errex:
            logger.error(errex)
            sys.exit(1)

        data = response.json()
        columns = [
            "id",
            "name",
            "absolute_magnitude_h",
            "kilometers estimated_diameter_max",
            "is_potentially_hazardous_asteroid", 
            "close_approach_date", 
            "relative_velocity kilometers_per_hour", 
            "miss_distance kilometers",  
            "orbiting_body",
        ]
        rows = []

        days = [(CURRENT_DATE - relativedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

        for day in days:
            asteroids_list = data["near_earth_objects"][day]
            for asteroid in asteroids_list:
                row = []
                for column in columns:
                    try:
                        row.append(find_key(asteroid, column))
                    except Exception as e:
                        logger.error(e)

                rows.append(row)

        try:
            df = pd.DataFrame(columns=columns, data= rows)
            df.to_csv(file_path)
        except OSError:
            os.mkdir(os.path.join(os.getcwd(), "data", "raw", CURRENT_DATE.strftime("%m")))
            df.to_parquet(file_path)

        return df.to_dict()
    
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    result = extract_data_from_api()
    print(type(result))