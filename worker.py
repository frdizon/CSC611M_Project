from textblob import TextBlob
from multiprocessing import Process, Lock, Value
import pika
import sys

# HELPERS: -------------------------------------------------------------

# Create a function to get the polarity
def getPolarity(text):
    return TextBlob(text).sentiment.polarity

# Process: -------------------------------------------------------------

class EvaluateTweetsProcess(Process):
    def __init__(self, tweets, pos_count, neu_count, neg_count):
        Process.__init__(self)
        self.tweets = tweets
        self.positiveCount = pos_count
        self.neutralCount = neu_count
        self.negativeCount = neg_count

    def run(self):
        for tweet in self.tweets['text']:
            polarityValue = getPolarity(tweet)
            if -0.05 <= polarityValue and polarityValue <= 0.05:
                self.neutralCount.value += 1
            elif polarityValue < -0.05:
                self.negativeCount.value += 1
            elif polarityValue > 0.05:
                self.positiveCount.value += 1

# Message Queue: ----------------------------------------------------------

class MessageQueue():
    def __init__(self, host):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()

    def __del__(self):
        self.connection.close()

    def dispatch(self, queue_name, results):
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=results,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            ))
    
    def __callback(self, ch, method, properties, body):
        # Initialize Polarity Categories count
        pos_count = Value('i', 0) # n < -0.05
        neu_count = Value('i', 0) # -0.05 <= n <= 0.05
        neg_count = Value('i', 0) # n > 0.05

        evalTweetsProcess = EvaluateTweetsProcess(
            body.decode(), pos_count, neu_count, neg_count)
        evalTweetsProcess.start()

        results = {
          positiveCount: pos_count,
          neutralCount: neu_count,
          negativeCount: neg_count
        }

        # dispatch back the results
        self.dispatch('results_queue', results)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def listen(self, queue_name):
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=queue_name, 
            on_message_callback=self.__callback)
        self.channel.start_consuming()

# -----------------------------------------------------------------------

if __name__ == '__main__':
    # initialize message queue
    mq = MessageQueue('localhost')
    mq.listen('dispatch_queue')
