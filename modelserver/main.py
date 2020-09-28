#!/usr/bin/env python
import pika
import time
import pickle
import sklearn
import pymongo
import sys
import os
import logging
import time
import time
import configparser
import argparse
from logging import getLogger, StreamHandler, DEBUG


def callback(ch, method, properties, body):
    logging.info(" [x] Received %r" % body.decode())
    idVal = body.decode()
    logging.info(" [x] Done")
    model = pickle.load(open('./app/model/model.pkl', 'rb'))

    values = list(mycol.find({"id":idVal},{"_id":0, "rateOfInterest":1,"salesIn1stMonth":1,"salesIn2ndMonth":1}))[0]

    predict_val = model.predict([[values['rateOfInterest'],values['salesIn1stMonth'],values['salesIn2ndMonth']]])
    query_val = {"id":idVal}
    logging.info("Prediction value" + str(predict_val[0]))
    pred_val_query = {"$set":{"prediction":predict_val[0]}}
    mycol.update_one(query_val, pred_val_query)
    ch.basic_ack(delivery_tag=method.delivery_tag)

myclient = pymongo.MongoClient("mongodb://dbserver:27017")
mydb = myclient["mydatabase"]
mycol = mydb["preddata"]
logging.basicConfig(level=20)

def main():

    logging.error("****************************")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    logging.info("model server - receiver")
    channel.queue_declare(queue='task_queue', durable=True)
    logging.info(' [*] Waiting for messages. To exit press CTRL+C')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    program_name = os.path.splitext(os.path.basename(__file__))[0]
    try:
        main()
    except Exception as e:
        logging.error(e)
        sys.exit(1)