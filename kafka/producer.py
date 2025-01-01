from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def create_producer():
    conf = {
        'bootstrap.servers': 'localhost:9092'
    }
    return Producer(**conf)


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_messages(producer, topic, num_messages=10):
    while True:
        for i in range(num_messages):
            name = 'govind'+str(i)
            producer.produce(topic, key=f'key-{i}', value=f'value-{name}', callback=delivery_report)
            producer.poll(0)
        producer.flush()



def create_topic(broker, topic_name, num_partitions=1, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': broker})

    topic_list = [NewTopic(topic_name, num_partitions, replication_factor)]
    fs = admin_client.create_topics(topic_list)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")


if __name__ == '__main__':
    producer = create_producer()
    produce_messages(producer, 'test_topic')
