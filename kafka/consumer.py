from confluent_kafka import Consumer, KafkaError


def create_consumer():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(**conf)


def consume_messages(consumer, topic):
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print(f'Received message: {msg.value().decode("utf-8")}')

    consumer.close()


if __name__ == '__main__':
    consumer = create_consumer()
    consume_messages(consumer, 'test_topic')
