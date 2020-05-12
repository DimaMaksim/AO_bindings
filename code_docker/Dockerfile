FROM registry.fozzy.lan/calcengine/flaskbasic
#FROM python:3.7.6
COPY . /app/

WORKDIR /app

RUN pip3 install -r requirements.txt
RUN ["apt-get", "update"]
#RUN ["apt-get", "-y", "install", "vim"]


CMD ["/bin/bash","-c","python3 bind_correcting.py & tail -f /dev/null"]
