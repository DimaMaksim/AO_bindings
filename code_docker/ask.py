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



filials=[2016,2407,2024,2016,2023,2024,2056,2113,2132,2115,2123,2122,2134,2481,2114,2133,2673,2112,2131,2151,2170,2154,2184]
start,end,n = "2020-03-15","2020-05-05",4
#for fil in filials:
mes=dict(filid=filials,start=start,end=end,procnum=n)
channel.basic_publish(exchange='',routing_key=config['rabbit']['in_queue'],body=json.dumps(mes))

connection.close()
