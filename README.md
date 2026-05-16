# real-time-surge-detection-system

A real-time backend system built using Spring Boot, Apache Kafka, and Kafka Streams to detect ride demand surges similar to Uber/Ola pricing systems.

## Tech Stack

- Java 21
- Spring Boot
- Apache Kafka
- Kafka Streams
- RocksDB

---

## Features

- Real-time ride request ingestion
- Zone-based ride aggregation
- Tumbling window processing
- Dynamic surge multiplier logic
- Cooldown mechanism using state stores
- Kafka topic-to-topic stream processing

---

## Kafka Streams Concepts Used

- KStream
- KTable
- Windowed Aggregation
- Stateful Processing
- RocksDB State Stores
- Stream Transformations

---

## System Flow

```text
Ride Requests
      ↓
ride-requests topic
      ↓
Kafka Streams Processor
      ↓
Windowed Aggregation
      ↓
Surge Detection + Cooldown Logic
      ↓
surge-events topic
```

---

## Sample Surge Event

```json
{
  "zoneId": "Delhi-CP",
  "requestCount": 12,
  "surgeMultiplier": 2.0
}
```

---

## Run Locally

### Start Kafka

```bash
brew services start kafka
```

### Create Topics

```bash
kafka-topics --create \
--topic ride-requests \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

```bash
kafka-topics --create \
--topic surge-events \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1
```

### Run Application

```bash
mvn spring-boot:run
```

---

## Learning Outcomes

This project helped in understanding:

- Real-time stream processing
- Stateful event-driven systems
- Kafka Streams topology design
- Windowing and aggregation
- Persistent state stores with RocksDB
