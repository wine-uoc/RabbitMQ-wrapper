import AMQPReceiverWrapper 
import AMQPSenderWrapper 
import time

def callback_func(channel, method, properties, body):
    print("channel = {}, method = {}, properties = {}, received body '{}'".format(channel, method, properties, body))

if __name__ == '__main__':
    receiver  = AMQPReceiverWrapper.AMQPReceiverWrapper()
    sender    = AMQPSenderWrapper.AMQPSenderWrapper()
    
    host      = "192.168.1.168"
    port      = 5672
    user      = "pi"
    password  = "wine4ever"
    queueName = "xaviqueue"

    #init sender 
    sender.createConnection(host, port, user, password)
    sender.createQueue(queueName, persistent=True)

    #init receiver
    receiver.createConnection(host, port, user, password)
    receiver.createQueue(queueName, persistent=True)
    receiver.setCallbackFunction(queueName, callback_func)
    #start the receiver thread
    receiver.start()
    
    for i in range(10):
        data = "xavi the best " + str(i)
        sender.sendAMQP('',queueName, data)
        print("sending data to queue " + data)    
        time.sleep(5)
    
    print("closing connection")
    sender.closeConnection()
    receiver.closeConnection()
    print("bye!")
    

