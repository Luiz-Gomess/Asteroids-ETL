import datetime
import os
import pandas as pd
import sys
from loguru import logger

logger.add("logs/transform.log")

def trasform(data):

    logger.info("Iniciando etapa de transformação")
    
    CURRENT_DATE = datetime.date.today()
    FILE = str(CURRENT_DATE) + " - processed_data.csv"

    output_path = os.path.join(os.getcwd(), "data", "processed")

    df = pd.DataFrame(data=data)

    try:
        df.rename(columns={"relative_velocity kilometers_per_second" : "relative_velocity_km/s",
                        "miss_distance kilometers":"miss_distance_kilometers",
                        "kilometers estimated_diameter_max": "estimated_diameter_kilometers"}, inplace=True)
    except Exception as ex:
        logger.error(ex)
        sys.exit(1)
    
    df["relative_velocity_km/s"] = df["relative_velocity_km/s"].apply(lambda x : round(float(x), 4))
    df["miss_distance_kilometers"] = df["miss_distance_kilometers"].apply(lambda x : round(float(x),2))
    df["miss_distance_kilometers"] = df["miss_distance_kilometers"].apply(lambda x : int(x))
    df["estimated_diameter_kilometers"] = df["estimated_diameter_kilometers"].apply(lambda x : round(float(x),4))

    
    try:
        os.makedirs(output_path, exist_ok=True)
        df.to_csv(os.path.join(output_path, FILE))

    except OSError as errex:
        logger.error(errex)
    

