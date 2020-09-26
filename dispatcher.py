import pandas as pd
import numpy as np 
import time
from multiprocessing import Value
import pika
import sys
import json

# -----------------------------------------------------------------------

class Results:
    def __init__(self, totalDispatched, timestart):
        self.timestart = timestart
        self.totalDispatched = totalDispatched
        self.totalBatchProcessed = 0
        self.positiveCount = 0 # n < -0.05
        self.neutralCount = 0 # -0.05 <= n <= 0.05
        self.negativeCount = 0 # n > 0.05

    def callback(self, ch, method, properties, body):
        sentResult = json.loads(body)
        print(sentResult)
        self.totalBatchProcessed += 1
        self.positiveCount += sentResult['positiveCount']
        self.neutralCount += sentResult['neutralCount']
        self.negativeCount += sentResult['negativeCount']

        print(str(self.totalBatchProcessed) + ' ' + str(self.totalDispatched))
        if self.totalBatchProcessed == self.totalDispatched:
            print('finish processing')
            self.print()
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def print(self):
        mostPolarity = 'Positive'
        mostPolarityValue = self.positiveCount
        if mostPolarityValue < self.neutralCount:
            mostPolarityValue = self.neutralCount
            mostPolarity = 'Neutral'
        if mostPolarityValue < self.negativeCount:
            mostPolarityValue = self.negativeCount
            mostPolarity = 'Negative'
        totalCount = self.positiveCount + self.neutralCount + self.negativeCount
        print('APPROACH 1 Results:') # TO DO: Show Percentage
        # print(mostPolarity + '( Positive: ' + str(positiveCount) + '(' + str((self.positiveCount/totalCount) * 100) + '%)' + ', Neutral: ' + str(self.neutralCount) + '(' + str((self.neutralCount/totalCount) * 100) + '%) , Negative: ' + str(self.negativeCount) + '(' + str((self.negativeCount/totalCount) * 100) + '%)' + ')')
        print('Time Taken: ' + str(time.time() - self.timestart) + 's')

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
            body=json.dumps(tweets.to_json()),
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
    fileName = 'SamsungDataFinal.csv'
    batchCount = 100
    timestart = time.time()

    # Read csv, put it in dataframe
    tweetsFulldf = pd.read_csv(fileName)

    # initialize message queue
    mq = MessageQueue('localhost')

    # dispatch batch tasks to workers
    # batches = np.array_split(df.to_numpy(), batchCount)
    index = 0
    rowCount = int(len(tweetsFulldf))

    for offsetIndex in range(0, rowCount, batchCount):
        index += 1
        tweetsDataBatch = tweetsFulldf[offsetIndex : offsetIndex+batchCount]
        mq.dispatch('dispatch_queue', tweetsDataBatch)
        print('batch ' + str(index) + ' sent')

    # for batch in batches:
    #     print(batch)
    #     mq.dispatch('dispatch_queue', batch)

    # initialize results
    result = Results(index, timestart)

    mq.listen('results_queue', result.callback)

    # print results after all tasks are done
    # result.print(timestart)
