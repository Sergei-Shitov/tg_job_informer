FROM python:3.10

COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

COPY ./app ./application
WORKDIR /application

CMD python ./db_init.py && python ./app.py