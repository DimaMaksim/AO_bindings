import numpy as np
import pika
import json
import yaml
with open("config.yaml") as f:
    config=yaml.full_load(f)


credentials = pika.PlainCredentials(username=config['rabbit']['user'], password=config['rabbit']['pwd'])
connection = pika.BlockingConnection(pika.ConnectionParameters(host=config['rabbit']['host'],\
                            port=config['rabbit']['port'],virtual_host=config['rabbit']['vhost'], credentials=credentials))
channel = connection.channel()
#channel.queue_declare(queue=config['rabbit']['in_queue'],durable=True)



filials=[2264,2269,2265,2226,2234,2229,2679,2256,2268,2257,\
        2266,2262,2275,2260,2231,2213,2221,2195,2201,2216]
start,end = "2020-03-26","2020-05-14"
#for fil in filials:
mes=dict(filid=filials,start=start,end=end)
channel.basic_publish(exchange='',routing_key=config['rabbit']['in_queue'],body=json.dumps(mes))

connection.close()
