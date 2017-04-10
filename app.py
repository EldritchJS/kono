# Based heavily on https://github.com/mattf/word-fountain

import argparse
import os
import time
import ujson
import requests

from kafka import KafkaProducer

parser = argparse.ArgumentParser(description='Kafka word fountain')
parser.add_argument('--servers', help='The bootstrap servers', default='localhost:9092')
parser.add_argument('--topic', help='Topic to publish to', default='word-fountain')
parser.add_argument('--rate', type=int, help='requests per hour', default=3)
parser.add_argument('--token', help='Your github API token', default='') 
parser.add_argument('--endpoint', help='Github endpoint', default='events')
args = parser.parse_args()

servers = os.getenv('SERVERS', args.servers).split(',')
topic = os.getenv('TOPIC', args.topic)
rate = int(os.getenv('RATE', args.rate))
token = os.getenv('TOKEN', args.token)
endpoint = os.getenv('ENDPOINT', args.endpoint)

producer = KafkaProducer(bootstrap_servers=servers)

tokenstr = 'token ' + token 
headers = {'Authorization': tokenstr}
url = 'https://api.github.com/orgs/radanalyticsio/' + endpoint


def handleEvents(response):
    if 'Link' in response.headers:
        resp = requests.get(url, headers=headers)
        handleEvents(resp)
    events = ujson.loads(response.content)
    for i in range(0,len(events)):
        producer.send(topic, str(events[i]['type']).strip())
    
while True:
    resp = requests.get(url, headers=headers)

    if '[200]' in str(resp):
        events = ujson.loads(resp.content)
        # handleEvents(resp) # will need to traverse pagination
        for i in range(0,len(events)):
            producer.send(topic, str(events[i]['type']).strip())

    time.sleep(70.0)

