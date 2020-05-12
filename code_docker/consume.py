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
#channel.queue_declare(queue=config['rabbit']['out_queue'],durable=True)


def callback(ch, method, properties, body):
    mes=json.loads(body)
    print(mes)
    ####channel.basic_publish(exchange='',routing_key='DigitalCore.jobs.atomgen',body=json.dumps(par))


channel.basic_consume(queue=config['rabbit']['out_queue'], on_message_callback=callback, auto_ack=True)


channel.start_consuming()
