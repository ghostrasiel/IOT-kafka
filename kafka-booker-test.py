from kafka import KafkaProducer
import time
import random
import sys
import argparse

producer = KafkaProducer(bootstrap_servers='localhost:9092')
i = 0
parser = argparse.ArgumentParser()
parser.add_argument('--machine', type=str)
args = parser.parse_args()
machine = args.machine
while True:
    text = str({"machine": machine,
                "timestamp": int(time.mktime(time.localtime())),
                "temperature": random.randint(-1, 100),
                "humidity": random.randint(20, 100)
                })
    producer.send(f'test-IOT.{machine}', str.encode(text)).get(timeout=60)
    print(text)
    i += 1
    time.sleep(5)
