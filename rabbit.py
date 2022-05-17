import pika
import time
import json
from random import randrange
from augment_service import augment_service

def callback(ch, method, properties, body):
    historic = eval(body)
    print(historic)
    augment_service(historic)
    print(" [x]Received %r" % body)
   
    print(" [x] Done")
    ch.basic_ack(delivery_tag = method.delivery_tag)


def start_queue():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='augments_queue')
        
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume('augments_queue',callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()