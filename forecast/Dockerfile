FROM python:3

WORKDIR /usr/src/app

COPY ./forecast/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ./forecast/run.py run.py

CMD [ "python", "-u", "./run.py" ]
