import requests
import json
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName('test_classification_api') \
    .master("local[*]") \
    .getOrCreate()

path_to_data = "/Users/orestchukla/Desktop/platform-for-consumer-engagement-strategy/data/data_to_test_model.csv"

input_df = spark.read.csv(path_to_data, header=True, inferSchema=True)

df = input_df.toPandas()

records = df.to_dict('records')

url = 'http://127.0.0.1:5000/api/v1/global/classification'
headers = {'Content-Type': 'application/json'}
data = {
    'PersonalAPIKey': '60ea5d4d-34d4-49bb-b39c-7162a48e5396',
    'Records': records
}
response = requests.post(url, headers=headers, data=json.dumps(data))

if response.status_code == 200:
    response_data = response.json()
    processed_data = response_data['data']

    processed_df = spark.createDataFrame(processed_data)
    processed_df.show(100, False)
else:
    print(f"Request failed with status code {response.status_code}. {response.json()['error']}")

