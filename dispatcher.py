import pandas as pd
import numpy as np 
import time
from multiprocessing import Process
import pika
import sys
import json

# -----------------------------------------------------------------------

class Results:
    def __init__(self, batchCount, timestart):
        self.timestart = timestart
        self.batchCount = batchCount
        self.totalBatchProcessed = 0
        self.positiveCount = 0 # n < -0.05
        self.neutralCount = 0 # -0.05 <= n <= 0.05
        self.negativeCount = 0 # n > 0.05

    def callback(self, ch, method, properties, body):
        sentResult = json.loads(body)
        self.totalBatchProcessed += 1
        print('Recieved(' + str(self.totalBatchProcessed) + ' of ' +  str(self.batchCount) + '): '
                + str(sentResult))
        self.positiveCount += sentResult['positiveCount']
        self.neutralCount += sentResult['neutralCount']
        self.negativeCount += sentResult['negativeCount']
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if self.totalBatchProcessed == self.batchCount:
            self.print(ch)
            while ch._consumer_infos:
                ch.connection.process_data_events(time_limit=1) # 1 second
                ch.stop_consuming('results_queue')
                ch.close()            

    def print(self, ch):
        mostPolarity = 'Positive'
        mostPolarityValue = self.positiveCount
        if mostPolarityValue < self.neutralCount:
            mostPolarityValue = self.neutralCount
            mostPolarity = 'Neutral'
        if mostPolarityValue < self.negativeCount:
            mostPolarityValue = self.negativeCount
            mostPolarity = 'Negative'
        totalCount = self.positiveCount + self.neutralCount + self.negativeCount
        print('Results:')
        print(mostPolarity + ' (Positive: ' + str(self.positiveCount)
            + '(' + str((self.positiveCount/totalCount) * 100) + '%)'
            + ', Neutral: ' + str(self.neutralCount) + '(' + str((self.neutralCount/totalCount) * 100)
             + '%) , Negative: ' + str(self.negativeCount) + '(' + str((self.negativeCount/totalCount) * 100) + '%)' + ')')
        print('Time Taken: ' + str(time.time() - self.timestart) + 's')

# Message Queue: ----------------------------------------------------------

class MessageQueue:
    def __init__(self, host):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()

    def __del__(self):
        self.connection.close()

    def listen(self, queue_name, cb):
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=queue_name, 
            on_message_callback=cb)
        self.channel.start_consuming()

# -----------------------------------------------------------------------

class BatchSenderProcess(Process):
    def __init__(self, host, tweets, batchSize):
        Process.__init__(self)
        self.tweets = tweets
        self.batchSize = batchSize
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='dispatch_queue', durable=True)

    def run(self):
        rowCount = int(len(self.tweets))
        batchesIndex = range(0, rowCount, batchSize)
        index = 0
        for offsetIndex in batchesIndex:
            index += 1
            tweetsDataBatch = tweetsFulldf[offsetIndex : offsetIndex+batchSize]
            self.channel.basic_publish(
                exchange='',
                routing_key='dispatch_queue',
                body=json.dumps(tweetsDataBatch.to_json()),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                ))
            print('batch ' + str(index) + ' of ' + str(len(batchesIndex))  +' sent')
        self.connection.close()

    def listen(self, queue_name, cb):
        result = Results(index, timestart)
        mq.listen('results_queue', result.callback)

# -----------------------------------------------------------------------

if __name__ == '__main__':
    # PARAMS SET:
    fileName = 'SamsungDataFinal.csv'
    batchSize = 1000
    timestart = time.time()

    # Read csv, put it in dataframe
    tweetsFulldf = pd.read_csv(fileName)

    # initialize message queue
    mq = MessageQueue('localhost')

    # dispatch batch tasks to workers
    batchSenderProcess = BatchSenderProcess('localhost', tweetsFulldf, batchSize)
    batchSenderProcess.run()

    # initialize results
    batchCount = len(range(0, int(len(tweetsFulldf)), batchSize))
    result = Results(batchCount, timestart)
    mq.listen('results_queue', result.callback)
