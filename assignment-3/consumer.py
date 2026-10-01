import json
import logging
import urllib.parse
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 1. MongoDB Credentials & Atlas Configuration
username = urllib.parse.quote_plus("ShibangiPipeline")
password = urllib.parse.quote_plus("Test12345")
cluster_host = "clustershibangi.cpuw87u.mongodb.net"

# Cleaner URI without appended query parameters
ATLAS_URI = f"mongodb+srv://{username}:{password}@{cluster_host}/"

# 2. Kafka Configuration
KAFKA_BROKER = "localhost:9092"
TOPICS = ["fleet-telemetry-stream", "inventory-checkout-stream"]


def initialize_mongo():
    """Initializes and verifies connection to MongoDB Atlas."""
    try:
        # Pass authSource explicitly as a MongoClient parameter
        client = MongoClient(
            ATLAS_URI,
            authSource="admin",
            retryWrites=True,
            w="majority",
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000
        )
        
        # Test connection with a ping command against the admin DB
        client.admin.command("ping")
        logging.info("✅ Successfully connected and authenticated to MongoDB Atlas!")
        return client["quick_commerce_db"]
    except Exception as e:
        logging.error(f"❌ Failed to connect to MongoDB Atlas: {e}")
        raise e


def initialize_kafka_consumer():
    """Initializes Kafka Consumer for streaming topics."""
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="quick-commerce-consumer-group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        logging.info(f"✅ Subscribed to Kafka topics: {TOPICS}")
        return consumer
    except Exception as e:
        logging.error(f"❌ Failed to connect to Kafka: {e}")
        raise e


def process_and_store_events(db, consumer):
    """Consumes incoming messages from Kafka and inserts them into MongoDB Atlas collections."""
    fleet_collection = db["fleet_telemetry"]
    checkout_collection = db["inventory_checkouts"]

    print("\n🚀 Stream Consumer Active. Listening for events and writing to MongoDB Atlas...\n")

    for message in consumer:
        event = message.value
        topic = message.topic

        try:
            if topic == "fleet-telemetry-stream":
                result = fleet_collection.insert_one(event)
                logging.info(f"[TELEMETRY] Inserted | Vehicle: {event.get('vehicle_id')} | Battery: {event.get('battery_level')}% | Doc ID: {result.inserted_id}")

            elif topic == "inventory-checkout-stream":
                result = checkout_collection.insert_one(event)
                logging.info(f"[CHECKOUT]  Inserted | Order: {event.get('order_id')} | Status: {event.get('status')} | Doc ID: {result.inserted_id}")

        except PyMongoError as e:
            logging.error(f"Failed to insert document into MongoDB: {e}")


def main():
    try:
        db = initialize_mongo()
        consumer = initialize_kafka_consumer()
        process_and_store_events(db, consumer)
    except Exception as e:
        logging.critical(f"Pipeline execution stopped due to error: {e}")


if __name__ == "__main__":
    main()