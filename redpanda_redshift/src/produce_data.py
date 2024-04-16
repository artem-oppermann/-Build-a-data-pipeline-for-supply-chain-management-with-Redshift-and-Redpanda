import csv
import json
from confluent_kafka import Producer

# Kafka producer configuration
config = {
    'bootstrap.servers': 'localhost:19092',  # Update with your Redpanda server address
    'client.id': 'supply-chain-producer'
}

producer = Producer(config)

topic_name = 'supply_chain_data'  # Update with your topic name

# Define your schema for the supply chain data
schema = {
    "type": "struct",
    "fields": [
        {"field": "supplier_id", "type": "string"},
        {"field": "delivery_timestamp", "type": "string"},
        {"field": "production_timestamp", "type": "string"},
        {"field": "vehicle_timestamp", "type": "string"},
        {"field": "material_delivered", "type": "string"},
        {"field": "quantity", "type": "string"},
        {"field": "delivery_status", "type": "string"},
        {"field": "production_line_id", "type": "string"},
        {"field": "operation_efficiency", "type": "string"},
        {"field": "downtime_minutes", "type": "string"},
        {"field": "vehicle_id", "type": "string"},
        {"field": "latitude", "type": "string"},
        {"field": "longitude", "type": "string"},
        {"field": "environmental_conditions", "type": "string"}
    ]
}

def produce_message(record):
    producer.poll(0)
    message = {
        "schema": schema,
        "payload": record
    }
    producer.produce(topic=topic_name, value=json.dumps(message))
    producer.flush()

def produce_from_csv(file_path):
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            produce_message(row)

csv_file_path = 'supply_chain_data.csv'  # Update this with the path to your CSV file
produce_from_csv(csv_file_path)
