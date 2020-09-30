from fastapi import FastAPI
import pika
import pymongo
import uuid
from pydantic import BaseModel
import uvicorn
import threading
import pika
from time import sleep
import uuid

app = FastAPI()

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
    corr_id = mqClient.send_request(id)
    print(" [x] Sent %r" % id)
    return 1


class MQClient(object):
    internal_lock = threading.Lock()
    queue = {}

    def __init__(self, queue_id):

        self.queue_id = queue_id
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(queue="", durable=True)
        self.callback_queue = result.method.queue
        thread = threading.Thread(target=self._process_data_events)
        thread.setDaemon(True)
        thread.start()


    def _process_data_events(self):
        self.channel.basic_consume(on_message_callback=self._on_response, auto_ack=True, queue=self.callback_queue)

        while True:
            with self.internal_lock:
                self.connection.process_data_events(10)
                sleep(25)

    def _on_response(self, ch, method, props, body):
         self.queue[props.correlation_id] = body
         print(body)


    def send_request(self, payload):
        corr_id = str(uuid.uuid4())
        self.queue[corr_id] = None
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_id,
                                   properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=corr_id),
                                   body=payload)
        return corr_id

    def getResults(self, payload):
        print(self.queue)
        return_value = self.queue[payload]
        print("returned"
              )

        return return_value

mqClient = MQClient("ml_queue")

'''
if __name__ == "__main__":
    rpcClient = MQClient("ml_queue")
    uvicorn.run(app, host="0.0.0.0")
'''