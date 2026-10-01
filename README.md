\# EV Quick-Commerce Real-Time Stream Analytics Pipeline



An end-to-end real-time stream processing and analytics pipeline designed for quick-commerce electric vehicle (EV) fleet telemetry and automated inventory checkout tracking. Built with Apache Kafka, Python, MongoDB Atlas, Docker, and Power BI.



\---



\## 🏗️ Architecture \& Pipeline Flow



1\. \*\*Ingestion Layer\*\*: Simulated telemetry and inventory checkouts generated via Python producers.

2\. \*\*Message Broker\*\*: Apache Kafka managed via Docker Compose (`Zookeeper` + `Kafka Broker`).

3\. \*\*Stream Processing\*\*: PyMongo-integrated Python consumer reading streaming topics with error handling and backoff mechanics.

4\. \*\*Data Persistence\*\*: Cloud MongoDB Atlas database (`quick\_commerce\_db`) holding collections for `fleet\_telemetry` and `inventory\_checkouts`.

5\. \*\*Analytics \& Visualization\*\*: Power BI dashboards linked to MongoDB Atlas for real-time fleet health, battery monitoring, and order fulfillment tracking.



\---



\## 📂 Project Structure

ev-quickcommerce-pipeline/

│

├── assignment-1/

│   ├── docker-compose.yml          # Kafka \& Zookeeper container orchestration

│   └── assignment-1.pdf            # Architecture design \& strategy document

│

├── assignment-2/

│   └── producer.py                 # Multi-topic event stream generator (Telemetry \& Checkouts)

│

├── assignment-3/

│   ├── consumer.py                 # Stream processing engine with MongoDB Atlas integration

│   ├── test\_conn.py                # Database connection and authentication validator

│   └── pipeline\_test.py            # Unit testing suite for stream event schema validation

│

├── fleet\_telemetry\_sample.csv      # Sample EV telemetry data

├── inventory\_checkout\_sample.csv   # Sample checkout order events

├── requirements.txt                # Python environment dependencies

└── README.md                       # Complete project documentation





\---

\## 🚀 Phase Breakdown \& Module Details



\### Assignment 1: Architecture \& Kafka Broker Setup

\- Designed multi-topic messaging strategy for high-frequency EV telemetry (`fleet-telemetry-stream`) and discrete inventory events (`inventory-checkout-stream`).

\- Configured local containerized message broker using Docker Compose for Zookeeper and Kafka services.



\### Assignment 2: Event Producers \& Data Streaming

\- Implemented `assignment-2/producer.py` to stream synthetic and CSV-backed telemetry and checkout logs to Kafka topics.

\- Handled payload serialization, timestamps, dynamic telemetry calculations (battery drop rate, location GPS, speed), and fulfillment status changes (`PACKED`, `DISPATCHED`, `DELIVERED`).



\### Assignment 3: MongoDB Atlas Integration \& Processing

\- Developed robust streaming consumer (`assignment-3/consumer.py`) to parse incoming Kafka streams and dynamically route messages to MongoDB Atlas.

\- Configured secure authentication against `clustershibangi.cpuw87u.mongodb.net` targeting `quick\_commerce\_db`.

\- Established schema validation tests (`pipeline\_test.py`) and connection verification tools (`test\_conn.py`).



\---



\## 🛠️ Setup \& Running Instructions



\### 1. Prerequisites

\- Python 3.10+

\- Docker Desktop

\- MongoDB Atlas Account



\### 2. Environment Setup

```powershell

pip install -r requirements.txt

Start Kafka Infrastructure (Assignment 1)

PowerShell

docker-compose -f assignment-1/docker-compose.yml up -d

4\. Start Event Producer (Assignment 2)

PowerShell

python assignment-2/producer.py

5\. Start Stream Consumer (Assignment 3)

PowerShell

python assignment-3/consumer.py





Analytics \& Storage

Database: quick\_commerce\_db



Collections: fleet\_telemetry, inventory\_checkouts



Cluster: clustershibangi.cpuw87u.mongodb.net

