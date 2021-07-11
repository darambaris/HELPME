import asyncio
from dataclasses import asdict, dataclass, field
from io import BytesIO
import random

from confluent_kafka import Producer
from faker import Faker
from fastavro import parse_schema, writer


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"

@dataclass
class ClickAttribute:
    element: str = field(default_factory=lambda: random.choice(["div", "a", "button"]))
    content: str = field(default_factory=faker.bs)
    
    click_attribute_schema = {
        "name": "ClickAttributes",
        "type": "record",
        "fields": [
            {"name": "element", "type": "string"},
            {"name": "content", "type": "string"},
        ]
    } 

    @classmethod
    def attributes(self):
        return {faker.uri_page(): ClickAttribute() for _ in range(random.randint(1, 5))}


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))
    attributes: dict = field(default_factory=ClickAttribute.attributes)
    
    click_event_schema = {
        "type": "record",
        "name": "clickEvent",
        "namespace": "com.udacity.lessons.avro",
        "fields": [
            {"name": "email", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "uri", "type": "string"},
            {"name": "number", "type": "int"},
            {
                "name": "attributes", 
                "type": {
                    "type": "map",
                    "values": ClickAttribute.click_attribute_schema
                }
            }
        ]
    }

    def serialize(self):
        """Serializes the ClickEvent for sending to Kafka"""
        schema = parse_schema(schema=ClickEvent.click_event_schema)
        out = BytesIO()
        writer(out, schema, [asdict(self)])

        return out.getvalue()


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, ClickEvent().serialize())
        await asyncio.sleep(1.0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        asyncio.run(produce_consume("consumer.serialization.clicks"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    await asyncio.create_task(produce(topic_name))


if __name__ == "__main__":
    main()
