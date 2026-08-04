import time
import pika
from pymongo import MongoClient

from parser import parse_log

credentials = pika.PlainCredentials(
    "rabbituser",
    "rabbit1234"
)

# -----------------------
# Connect to RabbitMQ
# -----------------------

connection = None

while connection is None:
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host="rabbitmq",
                credentials=credentials
            )
        )
        print("Connected to RabbitMQ!")

    except pika.exceptions.AMQPConnectionError:
        print("RabbitMQ not ready... retrying")
        time.sleep(5)

channel = connection.channel()

channel.queue_declare(
    queue="log_queue",
    durable=True
)

# -----------------------
# Connect to MongoDB
# -----------------------

mongo = MongoClient("mongodb://mongodb:27017/")

db = mongo["minisplunk"]

collection = db["logs"]

print("Connected to MongoDB!")

# -----------------------
# Callback
# -----------------------

def callback(ch, method, properties, body):

    log = body.decode()

    parsed = parse_log(log)

    if parsed:

        collection.insert_one(parsed)

        print("\n===== STORED =====")
        print(parsed)

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    queue="log_queue",
    on_message_callback=callback
)

print("Worker waiting for logs...")

channel.start_consuming()
