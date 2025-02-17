from fastapi import FastAPI
from kafka import KafkaProducer
import requests
import json

app = FastAPI()

print("Starting Kafka Producer...")
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",  # Ensure this is correct
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

API_URL = "https://www.alphavantage.co/query"
API_KEY = "SOU9O5LJWN2GY2J5"

@app.get("/fetch-data/{symbol}")
def fetch_data(symbol: str):
    print(f"Fetching data for: {symbol}")  # Debugging
    response = requests.get(API_URL, params={
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "1min",
        "apikey": API_KEY
    })
    data = response.json()
    print(f"Data received: {data}")  # Debugging
    producer.send("financial_data", data)
    print("Data sent to Kafka")  # Debugging
    return {"status": "Data sent to Kafka"}

print("FastAPI server is ready!")

