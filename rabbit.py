import pika
import time
import json
from random import randrange

def callback(ch, method, properties, body):
    teste = eval(body)
    print(teste,'RECEIVER')
    print(" [x]xxxx Received %r" % body)
    time.sleep(randrange(0, 5))
    print(" [x] Done")
    ch.basic_ack(delivery_tag = method.delivery_tag)


def start_queue():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')
        
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume('hello',callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()