from time import sleep
from json import dumps
from kafka import KafkaProducer
import random
from datetime import datetime

def generate_data():
    # Get current date and time
    current_date = datetime.now().strftime("%m/%d/%Y")
    current_time = datetime.now().strftime("%H:%M:%S")

    # Simulate generating IoT data
    data = {
        "date": current_date,
        "humidity": str(random.uniform(20.0, 30.0)),
        "temperature": str(random.uniform(20.0, 50.0)),
        "time": current_time
    }
    return data

def publish_message(producer_instance, topic_name):
    try:
        data = generate_data()
        producer_instance.send(topic_name, value=dumps(data).encode('utf-8'))
        print("Message: ", data)
    except Exception as ex:
        print('Exception in publishing message:', ex)

if __name__ == '__main__':
    # Kafka producer configuration
    kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = 'topic_StreamingLab'
    
    # Simulate generating messages every 5 seconds
    while True:
        publish_message(kafka_producer, topic)
        sleep(5)
