from confluent_kafka import Producer

PERSONAL_ID = "joe" # To ensure all users read the entire topic

conf = {
    'bootstrap.servers': 'b-1.monstercluster1.6xql65.c3.kafka.eu-west-2.amazonaws.com:9092',  # This is the address of your Kafka broker(s)
    'client.id': 'retail_store_producer'
}

producer = Producer(conf)

transaction_data = {
    'product_name': 'rocket_ship',
    'category': 'toy',
    'amount': 10
}

# Convert the data to string format; often JSON is used
import json
transaction_string = json.dumps(transaction_data)

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Use the callback when producing a message
producer.produce('retail_product_topic', transaction_string, callback=delivery_report)
producer.flush()