#!/usr/bin/env python
import pika
import time
import pickle
import sklearn
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017")
mydb = myclient["mydatabase"]
mycol = mydb["preddata"]

def callback(ch, method, properties, body):
    print(" [x] Received %r" % body.decode())
    idVal = body.decode()
    print(" [x] Done")
    model = pickle.load(open('model/model.pkl', 'rb'))

    values = list(mycol.find({"id":idVal},{"_id":0, "rateOfInterest":1,"salesIn1stMonth":1,"salesIn2ndMonth":1}))[0]

    predict_val = model.predict([[values['rateOfInterest'],values['salesIn1stMonth'],values['salesIn2ndMonth']]])
    query_val = {"id":idVal}
    pred_val_query = {"$set":{"prediction":predict_val[0]}}
    mycol.update_one(query_val, pred_val_query)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def predict_process():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    channel.start_consuming()


if __name__ == "__main__":
    predict_process()
