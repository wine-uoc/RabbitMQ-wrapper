#!/usr/bin/env python
import pika
import sys
import threading

class AMQPReceiverWrapper(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
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

    ''' closes the AMQP connection with the server and stops the consumer'''
    def closeConnection(self):
        self.channel.stop_consuming()
        self.connection.close()

    ''' sets the callback fuction that will be called when a new message is received'''
    def setCallbackFunction(self, queueName, callbackf):
        self.callback=callbackf
        self.channel.basic_consume(queue=queueName, on_message_callback=self.callback, auto_ack=True)
    
    ''' start the thread!!'''
    def start(self):             
        # Call thread start
        threading.Thread.start(self)

    ''' this is the main thread loop'''
    def run(self):
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()


