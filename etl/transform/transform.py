import datetime
import os
import pandas as pd
from dateutil.relativedelta import relativedelta

def trasform(data, output_path = os.path.join(os.getcwd(), "data", "processed")):
    
    df = pd.DataFrame(data=data)

    df.rename(columns={"relative_velocity kilometers_per_second" : "relative_velocity_km/s",
                       "miss_distance kilometers":"miss_distance_kilometers",
                       "kilometers estimated_diameter_max": "estimated_diameter_kilometers"}, inplace=True)
    
    
    df["relative_velocity_km/s"] = df["relative_velocity_km/s"].apply(lambda x : round(float(x), 4))
    df["miss_distance_kilometers"] = df["miss_distance_kilometers"].apply(lambda x : round(float(x),2))
    df["estimated_diameter_kilometers"] = df["estimated_diameter_kilometers"].apply(lambda x : round(float(x),4))
    

    return df

