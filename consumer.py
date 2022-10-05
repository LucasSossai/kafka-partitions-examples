from confluent_kafka import Consumer, KafkaException

TOPIC = "topic_1_partition"


def consume_loop(consumer, topic=TOPIC):
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                print(
                    f"[{msg.topic()}][{msg.offset()}]: Received message {msg.value().decode('utf-8')} in partition {msg.partition()}"
                )

    except Exception as e:
        print(f"Received exception while consuming: {e}")

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "consumer-group-1",
    "session.timeout.ms": 6000,
    "auto.offset.reset": "latest",
}

consumer = Consumer(conf)
consume_loop(consumer)
