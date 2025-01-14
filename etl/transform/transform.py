import datetime
import os
import pandas as pd
from loguru import logger


def trasform(data):

    logger.info("Etapa de Transformação iniciada")
    
    CURRENT_DATE = datetime.date.today()

    filename = str(CURRENT_DATE) + " - processed_data.parquet"
    output_path = os.path.join(os.getcwd(), "data", "processed", CURRENT_DATE.strftime("%m"))


    df = pd.DataFrame(data=data)

    try:
        logger.info("Renomeando colunas")

        df.rename(columns={"relative_velocity kilometers_per_second" : "relative_velocity_km/s",
                        "miss_distance kilometers":"miss_distance_kilometers",
                        "kilometers estimated_diameter_max": "estimated_diameter_kilometers"}, inplace=True)
    except Exception as ex:
        logger.error(ex)
        raise Exception(ex)
        
    logger.info("Aplicando transformações")

    df["relative_velocity_km/s"] = df["relative_velocity_km/s"].apply(lambda x : round(float(x), 4))
    df["miss_distance_kilometers"] = df["miss_distance_kilometers"].apply(lambda x : round(float(x),2))
    df["miss_distance_kilometers"] = df["miss_distance_kilometers"].apply(lambda x : int(x))
    df["estimated_diameter_kilometers"] = df["estimated_diameter_kilometers"].apply(lambda x : round(float(x),4))

    
    try:
        os.makedirs(output_path, exist_ok=True)
        df.to_parquet(os.path.join(output_path, filename))

        return os.path.join(output_path, filename)

    except OSError as errex:
        logger.error(errex)
    
    finally:
        logger.info("Etapa de Transformação finalizada.")

    

