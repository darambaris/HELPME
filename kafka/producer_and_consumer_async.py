import asyncio
import logging

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "localhost:9092"
TOPIC_NAME = "my-first-python-topic"


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

        # TODO: Handle the message. Remember that you should:
        #   1. Check if the message is `None`
        #   2. Check if the message has an error: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.error
        #   3. If 1 and 2 were false, print the message key and value
        #       Key: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.key
        #       Value: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Message.value
        #
        if message is None:
            logging.info("No message received!")
        elif message.error() is not None:
            logging.error(f"Message had a error {message.error()}")
        else:
            print(f"Message key: {message.key()}, value: {message.value()}")

        await asyncio.sleep(1)


# Article about async programming (in Portuguese): https://bityli.com/vsEQ5
async def produce_consume():
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1  # block point coroutine
    await t2


def main():

    client = AdminClient({"bootstrap.servers": BROKER_URL})

    # create a new topic
    topic = NewTopic(topic=TOPIC_NAME, num_partitions=1, replication_factor=1)
    client.create_topics([topic])

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt:
        print("shutting down...")
    finally:
        client.delete_topics([topic])
        pass


if __name__ == "__main__":
    main()
