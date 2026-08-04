from fastapi import FastAPI, UploadFile, File
import pika

app = FastAPI()

RABBITMQ_HOST = "rabbitmq"

credentials = pika.PlainCredentials(
    "rabbituser",
    "rabbit1234"
)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials
    )
)

channel = connection.channel()

channel.queue_declare(queue="log_queue")

@app.get("/")
def home():
    return {"message": "MiniSplunk Gateway Running"}


@app.post("/ingest")
async def ingest(file: UploadFile = File(...)):

    contents = await file.read()

    logs = contents.decode().splitlines()

    for log in logs:
        channel.basic_publish(
            exchange="",
            routing_key="log_queue",
            body=log
        )

    return {
        "status": "success",
        "logs_received": len(logs)
    }
