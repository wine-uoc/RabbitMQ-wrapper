#!/usr/bin/env python
import pika
import sys

class AMQPSenderWrapper(object):

    def __init__(self):
        self.connection = None
        self.channel    = None
    
    ''' creates a connection to the rabbitmq server'''
    def createConnection(self, host='localhost', port=5672, user="", password=""):    
        self.credentials = pika.PlainCredentials(user, password)
        self.connection  = pika.BlockingConnection(pika.ConnectionParameters(host=host, port=port,credentials=self.credentials))   
        self.channel     = self.connection.channel()
    
    ''' creates a new queue with or without persistence'''
    def createQueue(self, queueName, persistent=True):
        self.channel.queue_declare(queue=queueName, durable=persistent)

    ''' closes the AMQP connection with the server'''
    def closeConnection(self):
        self.connection.close()
   
    ''' sends an AMQP message. delivery_mode = 2 -> persistent message '''
    def sendAMQP(self, exchange, routing_key, message, delivery_mode = 2 ):
        self.channel.basic_publish(exchange=exchange, 
                               routing_key=routing_key, 
                               body=message,  
                               properties=pika.BasicProperties( delivery_mode=2,))
