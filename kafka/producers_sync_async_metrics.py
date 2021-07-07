##########################################
# Metrics: / macOS X system
# async: 2.000 messages in 31 seconds / first 100 messages consumed: 38
# sync: 2.000 messages in 32 seconds / first 100 messages consumed: 24
##########################################

from dataclasses import dataclass, field, asdict
from datetime import datetime
import json
import random
import logging

import asyncio
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker  # generate fake data


BROKER_URL = "localhost:9092"
PRODUCER_TYPE = "sync"
faker = Faker()


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        return json.dumps(asdict(self))


async def produce_sync(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({
        "bootstrap.servers": BROKER_URL,
        "compression.type": "lz4",
        "linger.ms": 3,  # default is 5ms
        "batch.num.messages": 5000,  # default is 10,000
        "batch.size": 500000  # default is 1,000,000
    })

    curr_iteration = 0
    start_time = datetime.utcnow()

    while True:
        # produce message to topic
        p.produce(topic=topic_name, value=Purchase().serialize())
        p.flush()

        if curr_iteration % 1000 == 0:
            elapsed = (datetime.utcnow() - start_time).seconds
            print(
                f"Messages sent per second: {curr_iteration} | Total elapsed seconds: {elapsed}")

        curr_iteration += 1

        await asyncio.sleep(0.01)


async def produce_async(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({
        "bootstrap.servers": BROKER_URL,
        "compression.type": "lz4",
        "linger.ms": 3,  # default is 5ms
        "batch.num.messages": 5000,  # default is 10,000
        "batch.size": 500000  # default is 1,000,000
    })

    curr_iteration = 0
    start_time = datetime.utcnow()

    while True:
        # produce message to topic
        p.produce(topic=topic_name, value=Purchase().serialize())
        p.flush()

        if curr_iteration % 1000 == 0:
            elapsed = (datetime.utcnow() - start_time).seconds
            print(
                f"Messages sent per second: {curr_iteration} | Total elapsed seconds: {elapsed}")

        curr_iteration += 1

        await asyncio.sleep(0.01)


def create_topic(client, topic_name):
    """Creates the topic with the given topic name"""
    topic_config = {
        "cleanup.policy": "delete",
        "compression.type": "lz4",
        "delete.retention.ms": 2000,
        "file.delete.delay.ms": 2000,
    }
    futures = client.create_topics(
        [
            NewTopic(
                topic=topic_name,
                num_partitions=1,
                replication_factor=1,
                config=topic_config,
            )
        ]
    )

    for topic, future in futures.items():
        try:
            future.result()
            print("topic created")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")
            raise


def topic_exists(client, topic_name):
    """Checks if the given topic exists"""
    # returns a dict with metadata
    cluster_metadata = client.list_topics(timeout=3)

    return cluster_metadata.topics.get(topic_name) is not None


async def _consume(topic_name):
    """Consumes produced messages"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])
    
    num_consumed = 0
    start_time = datetime.utcnow()
    
    while True:
        msg = c.consume(timeout=0.001)
        if msg:
            num_consumed += 1
            if num_consumed % 100 == 0:
                elapsed = (datetime.utcnow() - start_time).seconds
                
                print(f"consumed {num_consumed} messages | Total elapsed seconds: {elapsed}")
        else:
            await asyncio.sleep(0.01)


async def produce(topic_name, producer_type):
    """Runs the Producer and Consumer tasks"""
    function_name = eval("produce_"+producer_type)
    tt = asyncio.create_task(function_name(topic_name))
    t2 = asyncio.create_task(_consume(topic_name))
    await tt  # blocks point coroutine
    await t2

def main():
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    topic_name = "my-producers-topic"

    # checks if the given topic exists or creates a new one
    if topic_exists(client, topic_name):
        logging.warn(f"{topic_name} is already exists.")
    else:
        create_topic(client, topic_name)

    try:
        asyncio.run(produce(topic_name, producer_type=PRODUCER_TYPE))
    except KeyboardInterrupt:
        print("shutting down...")
    finally:
        client.delete_topics(topics=[topic_name])


if __name__ == "__main__":
    main()
