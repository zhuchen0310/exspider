FROM python:3.6.5

# App Setup
WORKDIR /opt/exspider
ADD . /opt/exspider

# Env Setup
ENV PYTHONPATH=$PYTHONPATH:/opt/exspider
ENV PYTHONPATH=/opt/exspider:$PYTHONPATH
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

RUN pip install -r requirements.txt
RUN pip install "celery[redis,msgpack]"
RUN mkdir /opt/logs
