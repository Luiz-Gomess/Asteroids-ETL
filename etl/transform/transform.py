import datetime
import os
import pandas as pd
from dateutil.relativedelta import relativedelta
from etl.extract.find_key import find_key

def trasform(data, output_path = os.path.join(os.getcwd(), "data", "processed")):

    current_date = datetime.date.today()
    start_date = current_date - relativedelta(days=7)

    asteroids_list = data["near_earth_objects"][str(current_date)]

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

    for asteroid in asteroids_list:
        row = []
        for column in columns:
            row.append(find_key(asteroid, column))
        rows.append(row)
        

    # columns = list(map(lambda column : column.replace(" ", "_"), columns))

    arquivo = f"{start_date.day}-{current_date.day}_asteroids.csv"
    current_month = current_date.strftime("%m") + "/"
    path = "raw_data/" + current_month+ arquivo

    df = pd.DataFrame(columns=columns, data=rows)

    try:
        df.to_csv(path, index=False)
    except OSError:
        os.mkdir("raw_data/" + current_month)
        df.to_csv(path, index=False)
