#!/usr/bin/env python
import pika
import time
import pickle
import sklearn
import pymongo
import sys
import os
import time
import time
import configparser
import argparse
from logging import getLogger, StreamHandler, DEBUG


class Manager:
    def __init__(self, program_name):

        self.logger = getLogger()
        handler = StreamHandler()
        handler.setLevel(DEBUG)
        self.logger.setLevel(DEBUG)
        self.logger.addHandler(handler)
        self.logger.propagate = False
        self.program_name = program_name
        self.logger.info('########## INITIALISE #############')

    def __enter__(self):
        self.start = time.time()
        self.logger.info('############# START ###############')
        return self

    def __exit__(self, type, value, traceback):
        self.end = time.time()
        self.logger.info('############ FINISH ###############')
        self.logger.info("{}: {:.3f} sec consumed.".format(self.program_name, self.end - self.start))


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())
    idVal = body.decode()
    print(" [x] Done")
    model = pickle.load(open('./app/model/model.pkl', 'rb'))

    values = list(mycol.find({"id":idVal},{"_id":0, "rateOfInterest":1,"salesIn1stMonth":1,"salesIn2ndMonth":1}))[0]

    predict_val = model.predict([[values['rateOfInterest'],values['salesIn1stMonth'],values['salesIn2ndMonth']]])
    query_val = {"id":idVal}
    pred_val_query = {"$set":{"prediction":predict_val[0]}}
    mycol.update_one(query_val, pred_val_query)
    ch.basic_ack(delivery_tag=method.delivery_tag)

myclient = pymongo.MongoClient("mongodb://dbserver:27017")
mydb = myclient["mydatabase"]
mycol = mydb["preddata"]

def main():
    amqp_url = os.environ['AMQP_URL']
    print('URL: %s' % (amqp_url,))
    print("****************************")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='host.docker.internal'))
    channel = connection.channel()
    print("model server - receiver")
    channel.queue_declare(queue='task_queue', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    program_name = os.path.splitext(os.path.basename(__file__))[0]
    with Manager(program_name) as m:
        try:
            main()
        except Exception as e:
            m.logger.exception(e)
            sys.exit(1)