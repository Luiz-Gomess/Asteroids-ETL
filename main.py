import datetime
import json
import os
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from helpers.find_key import find_key
# from helpers.send_to_s3 import send_to_s3

load_dotenv()

current_date = datetime.date.today()
start_date = current_date - relativedelta(days=7)

API_KEY = os.getenv("API_KEY")
url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&api_key={API_KEY}"
print(url)

response = requests.get(url)
data = json.loads(response.content)

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
    

columns = list(map(lambda column : column.replace(" ", "_"), columns))

arquivo = f"{start_date.day}-{current_date.day}_asteroids.csv"
current_month = current_date.strftime("%m") + "/"
path = "raw_data/" + current_month+ arquivo

df = pd.DataFrame(columns=columns, data=rows)

try:
    df.to_csv(path, index=False)
except OSError:
    os.mkdir("raw_data/" + current_month)
    df.to_csv(path, index=False)

# try:
#     send_to_s3(arquivo)
# except Exception as e:
#     print(e)
    
    
