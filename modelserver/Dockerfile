FROM python:3.7-slim-buster

COPY requirements.txt /app/

COPY wait-for-it.sh /app/

RUN pip install -r /app/requirements.txt

COPY model/. /app/model/

COPY . /app

CMD ["python", "./app/main.py"]