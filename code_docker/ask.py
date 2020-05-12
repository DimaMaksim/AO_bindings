import numpy as np
import pika
import json
import yaml
with open("config.yaml") as f:
    config=yaml.full_load(f)


credentials = pika.PlainCredentials(username=config['rabbit']['user'], password=config['rabbit']['pwd'])
connection = pika.BlockingConnection(pika.ConnectionParameters(host=config['rabbit']['host'],\
                            port=config['rabbit']['port'],virtual_host=config['rabbit']['vhost'],  credentials=credentials))
channel = connection.channel()
#channel.queue_declare(queue=config['rabbit']['in_queue'],durable=True)



filials=[1991,2407]
start="2020-03-15"; end="2020-05-05"
for fil in filials:
    mes=dict(filid=fil,start=start,end=end)
    channel.basic_publish(exchange='',routing_key=config['rabbit']['in_queue'],body=json.dumps(mes))

connection.close()
