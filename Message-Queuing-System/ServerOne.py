#!/usr/bin/env python
import pika, time, json
HOST = "10.0.2.2"

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=HOST))
channel = connection.channel()

channel.queue_declare(queue='unsorted_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    lyst = json.loads(body.decode('utf-8'))
    print(" [x] Received %r... %r items received" % (lyst[:20],len(lyst)))
    print(" [x] Will begin sorting")
    ch.basic_ack(delivery_tag = method.delivery_tag)
    sort_list_and_send(lyst)

def sort_list_and_send(lyst):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=HOST))
    channel = connection.channel()
    channel.queue_declare(queue='sorted_queue', durable=True)
    lyst.sort()
    message = json.dumps(lyst)
    channel.basic_publish(exchange='',
                          routing_key='sorted_queue',
                          body=message,
                          properties=pika.BasicProperties(
                                  delivery_mode = 2, # make message persistent
                                  ))
    print(" [x] Sorted and sent %r... %r items sent" % (lyst[:20],len(lyst)))
    print(" [x] Done")
    connection.close()

channel.basic_qos(prefetch_count=1)
# tells RabbitMQ not to give more than one message to a worker at a time.
channel.basic_consume(callback,
                      queue='unsorted_queue')

channel.start_consuming()