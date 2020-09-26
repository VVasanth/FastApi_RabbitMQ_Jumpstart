from fastapi import FastAPI
import pika
import pymongo
import uuid
from pydantic import BaseModel

app = FastAPI()


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)


myclient = pymongo.MongoClient("mongodb://dbserver:27017")
mydb = myclient["mydatabase"]
mycol = mydb["preddata"]


class PredInput(BaseModel):
    rateOfInterest: int
    salesInIstMonth: int
    salesIn2ndMonth: int

@app.get("/root")
async def root():
    return {"message": "Hello World Test"}


@app.put("/predict/")
async def predict_items(predInput:PredInput):
    id = str(uuid.uuid4())
    mydict = {"id":id,"rateOfInterest":predInput.rateOfInterest, "salesIn1stMonth":predInput.salesInIstMonth, "salesIn2ndMonth":predInput.salesIn2ndMonth}
    x= mycol.insert(mydict)
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        body=id,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    print(" [x] Sent %r" % id)
    return 1

'''
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0")
'''