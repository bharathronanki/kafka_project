# from pydantic import BaseModel
from fastapi import FastAPI
from kafka import KafkaProducer
import pandas as pd
import json
import os

# Define the FastAPI app
app = FastAPI()

'''
# Define the message schema
class Message(BaseModel):
    id: int
    name: str
    text_message: str
'''

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:19092"],  # using the PLAINTEXT_HOST port of kafka1
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

data_dir = "./data"

for file in os.listdir(data_dir):
    if file.endswith(".csv") and file.startswith("chunk"):
        file_path = os.path.join(data_dir, file)
        print(f"Processing file: {file_path}")

        # Read CSV
        df = pd.read_csv(file_path)

        # Iterate through rows and send to Kafka
        for index, row in df.iterrows():
            message = row.to_dict()
            producer.send("messages_topic", message)
            print(f"Sent: {message}")

'''
@app.post("/send/")
def send_message(message: Message):
    producer.send("messages_topic", message.dict())
    return {"status": "Message sent", "message": message}
'''

producer.flush()
print("All messages sent to Kafka!")