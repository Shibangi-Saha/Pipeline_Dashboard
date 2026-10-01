import json
import time
import random
import psycopg2
from kafka import KafkaProducer, KafkaConsumer

print("--- 1. TESTING POSTGRESQL CONNECTION ---")
try:
    pg_conn = psycopg2.connect(
        dbname="quick_commerce_db",
        user="postgres",
        password="postgres_password",
        host="127.0.0.1",
        port=5432
    )
    pg_conn.autocommit = True
    cursor = pg_conn.cursor()
    print("[✓] Connected to PostgreSQL")
except Exception as e:
    print(f"[✗] DB Error: {e}")
    exit(1)

print("\n--- 2. PRODUCING TEST EVENTS ---")
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

statuses = ['DELIVERED', 'IN_TRANSIT', 'PREPARING', 'CANCELLED']
test_orders = []

for i in range(1, 6):
    order_id = f"TEST-{random.randint(1000, 9999)}"
    event = {
        "order_id": order_id,
        "customer_id": f"CUST-{random.randint(100, 999)}",
        "amount": round(random.uniform(50.0, 500.0), 2),
        "status": random.choice(statuses)
    }
    producer.send('orders_stream', event)
    test_orders.append(order_id)
    print(f"Pushed to Kafka: {event['order_id']}")

producer.flush()
print("[✓] Flush complete. Events sent to broker!")

print("\n--- 3. CONSUMING & INSERTING TO POSTGRESQL ---")
consumer = KafkaConsumer(
    'orders_stream',
    bootstrap_servers=['127.0.0.1:9092'],
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000,  # Stops waiting after 5s of inactivity
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

records_inserted = 0
for message in consumer:
    event = message.value
    print(f"Consumed: {event['order_id']}")
    
    insert_query = """
    INSERT INTO orders (order_id, customer_id, amount, status)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (order_id) DO NOTHING;
    """
    cursor.execute(
        insert_query, 
        (event['order_id'], event['customer_id'], event['amount'], event['status'])
    )
    records_inserted += 1

print(f"\n[✓] Finished processing. Inserted {records_inserted} rows into PostgreSQL!")
cursor.close()
pg_conn.close()