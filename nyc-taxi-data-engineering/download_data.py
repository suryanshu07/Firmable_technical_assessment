import os
import urllib.request

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
months = [f"{i:02d}" for i in range(1, 13)]

os.makedirs("data", exist_ok=True)

for m in months:
    file = f"yellow_tripdata_2023-{m}.parquet"
    url = base_url + file
    print(f"Downloading {file}...")
    urllib.request.urlretrieve(url, f"data/{file}")