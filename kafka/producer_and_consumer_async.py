import asyncio
import logging

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "localhost:9092"


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    while True:

        # produce message to topic
        message = f"Message {curr_iteration} sent to topic"
        p.produce(topic=topic_name, value=message)

        curr_iteration += 1
        await asyncio.sleep(1)


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer(
        {"bootstrap.servers": BROKER_URL, "group.id": "my-first-python-consumer-group"}
    )
    c.subscribe(topics=[topic_name])

    while True:
        # try to pool a message every 1 second
        message = c.poll(timeout=1.0)

        if message is None:
            logging.info("No message received!")
        elif message.error() is not None:
            logging.error(f"Message had a error {message.error()}")
        else:
            print(f"Message key: {message.key()}, value: {message.value()}")

        await asyncio.sleep(1)


# Article about async programming (in Portuguese): https://bityli.com/vsEQ5
async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1  # blocks point coroutine
    await t2


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
    cluster_metadata = client.list_topics(timeout=10)

    return cluster_metadata.topics.get(topic_name) is not None


def main():
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    topic_name = "my-custom-python-topic"

    # checks if the given topic exists or creates a new one
    if topic_exists(client, topic_name):
        logging.warn(f"{topic_name} is already exists.")
    else:
        create_topic(client, topic_name)

    try:
        asyncio.run(produce_consume(topic_name))
    except KeyboardInterrupt:
        print("shutting down...")
    finally:
        client.delete_topics(topics=[topic_name])
        pass


if __name__ == "__main__":
    main()
