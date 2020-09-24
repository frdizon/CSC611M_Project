import pandas as pd
import numpy as np 
import time
from multiprocessing import Value
import pika
import sys

# -----------------------------------------------------------------------

class Results:
    def __init__(self):
        self.positiveCount = Value('i', 0) # n < -0.05
        self.neutralCount = Value('i', 0) # -0.05 <= n <= 0.05
        self.negativeCount = Value('i', 0) # n > 0.05

    def callback(self, ch, method, properties, body):
        self.positiveCount.value += body.positiveCount.value
        self.neutralCount.value += body.neutralCount.value
        self.negativeCount.value += body.negativeCount.value
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def print(self, timestart):
        mostPolarity = 'Positive'
        mostPolarityValue = self.positiveCount.value
        if mostPolarityValue < self.neutralCount.value:
            mostPolarityValue = self.neutralCount.value
            mostPolarity = 'Neutral'
        if mostPolarityValue < self.negativeCount.value:
            mostPolarityValue = self.negativeCount.value
            mostPolarity = 'Negative'
        totalCount = self.positiveCount.value + self.neutralCount.value + self.negativeCount.value
        print('APPROACH 1 Results:') # TO DO: Show Percentage
        print(mostPolarity + '( Positive: ' + str(positiveCount.value) 
            + '(' + str((self.positiveCount.value/totalCount) * 100)    
            + '%)' + ', Neutral: ' + str(self.neutralCount.value) + '(' 
            + str((self.neutralCount.value/totalCount) * 100) + '%)'
            ', Negative: ' + str(self.negativeCount.value) + '(' 
            + str((self.negativeCount.value/totalCount) * 100) + '%)' + ')')
        print('Time Taken: ' + str(time.time() - timestart) + 's')

# Message Queue: ----------------------------------------------------------

class MessageQueue:
    def __init__(self, host):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()

    def __del__(self):
        self.connection.close()

    def dispatch(self, queue_name, tweets):
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=tweets,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))

    def listen(self, queue_name, cb):
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=queue_name, 
            on_message_callback=cb)
        self.channel.start_consuming()

# -----------------------------------------------------------------------

if __name__ == '__main__':
    # PARAMS SET:
    fileName = 'SamsungDataFinalX4.csv'
    batchCount = 10
    timestart = time.time()

    # Read csv, put it in dataframe
    df = pd.read_csv(fileName)

    # initialize results
    result = Results()

    # initialize message queue
    mq = MessageQueue('localhost')

    # dispatch batch tasks to workers
    batches = np.array_split(df.to_numpy(), batchCount)

    for batch in batches:
        print(batch)
        mq.dispatch('dispatch_queue', batch)

    mq.listen('results_queue', result.callback)

    # print results after all tasks are done
    result.print(timestart)
