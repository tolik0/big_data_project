from kafka import KafkaProducer
import json
import requests

# data stream
STREAM_URL = "http://stream.meetup.com/2/rsvps"

KAFKA_TOPIC = "events7h"
SERVERS = ['localhost:9092']

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=SERVERS,
                                  api_version=(0, 10), value_serializer=lambda v: v.encode('utf-8'))
        print("Connected to Kafka producer")
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def write_message(producer_instance, topic_name, message):
    try:
        producer_instance.send(topic_name,  value=message).add_errback(lambda x: print(x))
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


if __name__ == "__main__":
    producer = connect_kafka_producer()

    counter = 0

    r = requests.get(STREAM_URL, stream=True)
    for line in r.iter_lines():
        # filter out keep-alive new lines
        if line:
            counter += 1
            js = json.dumps(line.decode('utf-8'))
            write_message(producer, KAFKA_TOPIC, js)

        if counter % 100 == 0:
            print("Send {} jsons".format(counter))

    producer.flush()