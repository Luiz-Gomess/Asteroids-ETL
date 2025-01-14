import datetime
import os   
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from etl.extract.find_key import find_key
from loguru import logger


def extract(api_key):
    try:
        logger.info("Etapa de Extração iniciada.")

        CURRENT_DATE = datetime.date.today()
        START_DATE = CURRENT_DATE - relativedelta(days=7)
        FILE = str(CURRENT_DATE) + " - raw_data.parquet"
    
        file_path = os.path.join(os.getcwd(), "data", "raw", CURRENT_DATE.strftime("%m"), FILE )
        api_url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={START_DATE}&end_date={CURRENT_DATE}&api_key={api_key}"


        response = requests.get(api_url)
        response.raise_for_status()
        logger.info(f"{response.status_code} - Chamada de API bem-sucedida.")

        data = response.json()
        columns = [
            "id",
            "name",
            "absolute_magnitude_h",
            "kilometers estimated_diameter_max",
            "is_potentially_hazardous_asteroid", 
            "close_approach_date", 
            "relative_velocity kilometers_per_second", 
            "miss_distance kilometers",  
            "orbiting_body",
        ]
        rows = []

        days = [(CURRENT_DATE - relativedelta(days=i)).strftime("%Y-%m-%d") for i in range(7+1)]

        logger.info("Iniciando coleta.")
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
        logger.info("Coleta finalizada")

        try:
            df = pd.DataFrame(columns=columns, data= rows)
            df.to_parquet(file_path)
        except OSError:
            os.mkdir(os.path.join(os.getcwd(), "data", "raw", CURRENT_DATE.strftime("%m")))
            df.to_parquet(file_path)
        finally:
            logger.info(f"Arquivo salvo como {FILE}")

        logger.info("Etapa de Extração finalizada.")
        
        return df.to_dict()
    
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    result = extract()
