import argparse
from kafka import KafkaProducer
import threading
import pandas as pd
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic')
    parser.add_argument('--brokerlist')
    parser.add_argument('--source')
    args = parser.parse_args()
    topic = args.topic
    brokerlist = args.brokerlist
    source = args.source
    print("brokerlist: %s topic: %s source: %s" % (brokerlist, topic, source))
    

    jsonArr = []
    with open(source, 'r') as f:
        data = f.read()
        jsonArr = json.loads(data)
    
    producer = KafkaProducer(bootstrap_servers=brokerlist)
    
    i = 0
    for jsonObj in jsonArr:
        message = bytes(json.dumps(jsonObj), 'utf-8')
        producer.send(topic, message)
        i = i + 1
        if i % 10 == 0:
            print("send message: " + str(i))

main()
