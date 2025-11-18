import csv
import json
from confluent_kafka import Producer

# Kafka config
conf = {
    'bootstrap.servers': 'localhost:29092'
}

producer = Producer(conf)
topic = "azki_user_events"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message sent to {msg.topic()} [{msg.partition()}]")


with open("user_events.csv", "r") as f:
    reader = csv.DictReader(f)

    for row in reader:
        # Convert row (dict) to JSON
        json_value = json.dumps(row)

        producer.produce(
            topic=topic,
            value=json_value.encode("utf-8"),
            callback=delivery_report
        )
        producer.poll(0)

producer.flush()
