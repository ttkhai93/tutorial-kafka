from random import choice

from confluent_kafka import Producer

if __name__ == '__main__':
    config = {
        'bootstrap.servers': 'localhost:61848',
        'acks': 'all'
    }
    producer = Producer(config)

    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print('Produces event to topic {topic}: key = {key:12} value={value:12}'.format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    topic = 'purchases'
    user_ids = ['eabara', 'jsmith', 'sgarcia',
                'jbernad', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    for _ in range(10):
        user_id = choice(user_ids)
        product = choice(products)
        producer.produce(topic, product, user_id, callback=delivery_callback)

    producer.poll(10000)
    producer.flush()
