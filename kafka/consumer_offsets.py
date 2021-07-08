import asyncio

from confluent_kafka import Consumer, Producer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"
CONSUMER_TYPE = "poll"


async def consume_poll(topic_name):
    """Consumes data from the Kafka Topic"""
    
    # Sleep for a few seconds to give the producer time to create some data
    await asyncio.sleep(2.5)
    
    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "0",
            "auto.offset.reset": "earliest"
        }
    )
    c.subscribe([topic_name], on_assign=on_assign)

    while True:
        message = c.poll(timeout=1.0) # try to consume a message until one second.  
        _handle_message(message)
        await asyncio.sleep(0.1)

async def consume_consume(topic_name):
    """Consumes data from the Kafka Topic"""
    
    # Sleep for a few seconds to give the producer time to create some data
    await asyncio.sleep(10.5)
    
    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "0",
            "auto.offset.reset": "earliest"
        }
    )
    c.subscribe([topic_name], on_assign=on_assign)

    while True:
        messages = c.consume(10, timeout=1.0) # try to consume 10 messages until one second.
        for message in messages:
            _handle_message(message)
        await asyncio.sleep(0.01)

def _handle_message(message):
    """ handle message according to content"""
    if message is None:
        print("no message received by consumer")
    elif message.error() is not None:
        print(f"error from consumer {message.error()}")
    else:
        print(f"consumed message {message.key()}: {message.value()}")
        

def on_assign(consumer, partitions):
    """Callback for when topic assignment takes place"""
    
    # Set all partitions from topic beginning to get all the data.
    # OFFSET_BEGINNING is different to 0, because the 0 message can be deleted.
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING

    # Set the consumer partition assignment to the provided list of TopicPartition and start consuming.
    consumer.assign(partitions)


def main():
    """Runs the exercise"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    try:
        asyncio.run(produce_consume(topic_name="consumer_offsets_topic", consumer_type=CONSUMER_TYPE))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    curr_iteration = 0
    while True:
        p.produce(topic_name, f"iteration {curr_iteration}".encode("utf-8"))
        curr_iteration += 1
        await asyncio.sleep(0.1)


async def produce_consume(topic_name, consumer_type):
    """Runs the Producer and Consumer tasks"""
    function_name = eval("consume_"+consumer_type)
    
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(function_name(topic_name))
    await t1
    await t2


if __name__ == "__main__":
    main()
