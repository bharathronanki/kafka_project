from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import json

# Define the FastAPI app
app = FastAPI()

# Define the message schema
class Message(BaseModel):
    id: int
    name: str
    text_message: str

# Setup Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:19092"],  # using the PLAINTEXT_HOST port of kafka1
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

@app.post("/send/")
def send_message(message: Message):
    producer.send("messages_topic", message.dict())
    return {"status": "Message sent", "message": message}
