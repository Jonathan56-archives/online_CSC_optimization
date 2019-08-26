from typing import List
from fastapi import FastAPI
from pydantic import BaseModel
from influxdb import DataFrameClient
from datetime import datetime, timedelta
import logging
import pandas
import time
import json

# Influxdb connection
host='influxdb'
port=8086
user = 'root'
password = 'root'
dbname = 'csc'

app = FastAPI()
logger = logging.getLogger("api")

class BatteryOrder(BaseModel):
    startby: str
    endby: str
    min_kw: float
    max_kw: float
    max_kwh: float
    initial_kwh: float
    end_kwh: float
    eta: float

class ShapeableOrder(BaseModel):
    startby: str
    endby: str
    max_kw: float
    end_kwh: float

class DeferrableOrder(BaseModel):
    startby: str
    endby: str
    duration: int
    profile_kw: str


@app.get("/ping")
def ping():
    return {"Hello": "World"}


@app.put("/forecast")
def forecast(times: List[str], values: List[float]):
    df = pandas.DataFrame(
        index=pandas.DatetimeIndex(times).round('5T'),
        data={'uncontr': values})

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'uncontr')
    client.close()

    # Test
    logger.info(df)

    # Run optimization
    return {"status": "sucess"}


@app.put("/batteryorder")
def battery_order(order: BatteryOrder):
    # Convert start and end time in second since epoch
    order.startby = datetime.strptime(
        order.startby, '%Y-%m-%dT%H:%M:%SZ').timestamp()
    order.endby = datetime.strptime(
        order.endby, '%Y-%m-%dT%H:%M:%SZ').timestamp()

    # Create a dataframe to be saved to influxdb
    df = pandas.DataFrame(index=[datetime.now()],
                          data=json.loads(order.json()))
    logger.info(df)

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'bbook')
    client.close()

    # Run optimization
    return {"status": "sucess"}


@app.put("/shapeableorder")
def shapeable_order(order: ShapeableOrder):
    # Convert start and end time in second since epoch
    order.startby = datetime.strptime(
        order.startby, '%Y-%m-%dT%H:%M:%SZ').timestamp()
    order.endby = datetime.strptime(
        order.endby, '%Y-%m-%dT%H:%M:%SZ').timestamp()

    # Create a dataframe to be saved to influxdb
    df = pandas.DataFrame(index=[datetime.now()],
                          data=json.loads(order.json()))
    logger.info(df)

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'sbook')
    client.close()

    # Run optimization
    return {"status": "sucess"}


@app.put("/deferrableorder")
def deferrable_order(order: DeferrableOrder):
    # Convert start and end time in second since epoch
    order.startby = datetime.strptime(
        order.startby, '%Y-%m-%dT%H:%M:%SZ').timestamp()
    order.endby = datetime.strptime(
        order.endby, '%Y-%m-%dT%H:%M:%SZ').timestamp()

    # Create a dataframe to be saved to influxdb
    df = pandas.DataFrame(index=[datetime.now()],
                          data=json.loads(order.json()))
    logger.info(df)

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'dbook')
    client.close()

    # Run optimization
    return {"status": "sucess"}


# Move to its own file
def optimization():
    pass
