from confluent_kafka import Producer
import time


TOPIC = "topic_1_partition"


def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {str(msg)}: {str(err)}")
    else:
        print(
            f"[{msg.topic()}][{msg.offset()}]: Delivered message {msg.value().decode('utf-8')} to partition {msg.partition()}"
        )


def produce_messages(producer, nro_messages=100):
    for i in range(nro_messages):
        producer.produce(TOPIC, f"Message {i}", callback=acked)
        producer.poll(0)
        time.sleep(1)
    producer.flush()


conf = {"bootstrap.servers": "localhost:9092", "client.id": "producer"}
producer = Producer(conf)
produce_messages(producer)
