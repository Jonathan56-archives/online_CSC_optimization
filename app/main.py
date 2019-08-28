from typing import List
from fastapi import FastAPI
from pydantic import BaseModel
from influxdb import DataFrameClient
from datetime import datetime, timedelta
from v4norminf import maximize_self_consumption
import randomorders
import logging
import pandas
import time
import json

# Solving Pyomo problem of threads
# https://github.com/Pyomo/pyomo/issues/609
import pyutilib.subprocess.GlobalData
pyutilib.subprocess.GlobalData.DEFINE_SIGNAL_HANDLERS_DEFAULT = False

# Contact j.coignard@lancey.fr
# Note: struggled with timezone this is built for CEST (+2h)

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
    return {"status": "sucess"}


@app.post("/optimize")
def optimize():
    optimization()
    return {"status": "sucess"}


@app.put("/forecast")
def forecast(times: List[str], values: List[float]):
    df = pandas.DataFrame(
        index=pandas.DatetimeIndex(times).round('5T'),
        data={'uncontr': values})

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'uncontr')
    client.close()

    # Run optimization
    optimization()
    return {"status": "sucess"}


@app.put("/batteryorder")
def battery_order(order: BatteryOrder):
    # Convert start and end time in second since epoch
    # minus 2 hours is a work around #@?! timezone
    # times 1000 for milliseconds
    order.startby = (datetime.strptime(
        order.startby, '%Y-%m-%dT%H:%M:%SZ') -
        timedelta(hours=2)).timestamp() * 1000
    order.endby = (datetime.strptime(
        order.endby, '%Y-%m-%dT%H:%M:%SZ') -
        timedelta(hours=2)).timestamp() * 1000

    # Create a dataframe to be saved to influxdb
    df = pandas.DataFrame(
        index=[datetime.now().replace(second=0, microsecond=0)],
        data=json.loads(order.json()))

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'bbook')
    client.close()

    # Run optimization
    optimization()
    return {"status": "sucess"}


@app.post("/randombatteryorder")
def random_battery_order():
    # Retrieve random order
    df = randomorders.random_battery_orderbook()

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'bbook')
    client.close()

    # Run optimization
    optimization()
    return {"status": "sucess"}


@app.post("/removebatteryorder")
def remove_battery_order(t: str):
    # Create fake order with 0
    data = {'min_kw': [0.0],
            'max_kw': [0.0],
            'max_kwh': [0.0],
            'initial_kwh': [0.0],
            'end_kwh': [0.0]}
    # minus 2 hours is a work around #@?! timezone
    df = pandas.DataFrame(
        index=[datetime.strptime(t, '%Y-%m-%d %H:%M:%S') -
               timedelta(hours=2)],
        data=data)

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'bbook')
    client.close()

    # Run optimization
    optimization()
    return {"status": "sucess"}


@app.put("/shapeableorder")
def shapeable_order(order: ShapeableOrder):
    # Convert start and end time in second since epoch
    # minus 2 hours is a work around #@?! timezone
    # times 1000 for milliseconds
    order.startby = (datetime.strptime(
        order.startby, '%Y-%m-%dT%H:%M:%SZ') -
        timedelta(hours=2)).timestamp() * 1000
    order.endby = (datetime.strptime(
        order.endby, '%Y-%m-%dT%H:%M:%SZ') -
        timedelta(hours=2)).timestamp() * 1000

    # Create a dataframe to be saved to influxdb
    df = pandas.DataFrame(
        index=[datetime.now().replace(second=0, microsecond=0)],
        data=json.loads(order.json()))

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'sbook')
    client.close()

    # Run optimization
    optimization()
    return {"status": "sucess"}


@app.post("/randomshapeableorder")
def random_shapeable_order():
    # Retrieve random order
    df = randomorders.random_shapeable_orderbook()

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'sbook')
    client.close()

    # Run optimization
    optimization()
    return {"status": "sucess"}


@app.post("/removeshapeableorder")
def remove_shapeable_order(t: str):
    # Create fake order with 0
    data = {'max_kw': [0.0],
            'end_kwh': [0.0]}
    # minus 2 hours is a work around #@?! timezone
    df = pandas.DataFrame(
        index=[datetime.strptime(t, '%Y-%m-%d %H:%M:%S') -
               timedelta(hours=2)],
        data=data)

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'sbook')
    client.close()

    # Run optimization
    optimization()
    return {"status": "sucess"}


@app.put("/deferrableorder")
def deferrable_order(order: DeferrableOrder):
    # Convert start and end time in second since epoch
    # minus 2 hours is a work around #@?! timezone
    # times 1000 for milliseconds
    order.startby = (datetime.strptime(
        order.startby, '%Y-%m-%dT%H:%M:%SZ') -
        timedelta(hours=2)).timestamp() * 1000
    order.endby = (datetime.strptime(
        order.endby, '%Y-%m-%dT%H:%M:%SZ') -
        timedelta(hours=2)).timestamp() * 1000

    # Create a dataframe to be saved to influxdb
    df = pandas.DataFrame(
        index=[datetime.now().replace(second=0, microsecond=0)],
        data=json.loads(order.json()))

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'dbook')
    client.close()

    # Run optimization
    optimization()
    return {"status": "sucess"}


@app.post("/randomdeferrableorder")
def random_deferrable_order():
    # Retrieve random order
    TIMESTEP = 12
    df = randomorders.random_deferrable_orderbook(
        timestep=60/TIMESTEP)

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'dbook')
    client.close()

    # Run optimization
    optimization()
    return {"status": "sucess"}


@app.post("/removedeferrableorder")
def remove_deferrable_order(t: str):
    # Create fake order with 0
    data = {'duration': [1],
            'profile_kw': [[0.0]]}
    # minus 2 hours is a work around #@?! timezone
    df = pandas.DataFrame(
        index=[datetime.strptime(t, '%Y-%m-%d %H:%M:%S') -
               timedelta(hours=2)],
        data=data)

    # Open connection and write to DB
    client = DataFrameClient(host, port, user, password, dbname)
    client.write_points(df, 'dbook')
    client.close()

    # Run optimization
    optimization()
    return {"status": "sucess"}


@app.post("/savetotaldemand")
def save_total_demand():
    # Limit the number of call to avoid
    # high cardinality of influxdb tags
    # Query total demand data
    client = DataFrameClient(host, port, user, password, dbname)
    start = datetime.now()
    query = ("select * from contr " +
             "WHERE time >= '" +
             (start).strftime("%Y-%m-%dT%H:%M:%SZ") +
             "' AND time <= '" +
             (start +
              timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ") +
             "'")
    contr = client.query(query)['contr']

    # Save it to a different measurement
    client.write_points(
        contr, 'versioncontr',
        {'version': str(int(datetime.now().replace(
            second=0, microsecond=0).timestamp() * 1000))})
    client.close()
    return {"status": "sucess"}


# Move to its own file
def optimization():
    # Optimization timestep
    TIMESTEP = 12  # 5min interval (60/5)

    # Query uncontrolled demand
    # Note: uncontrolled demand is already on a 5min timestep
    client = DataFrameClient(host, port, user, password, dbname)
    start = datetime.now()
    query = ("select * from uncontr " +
             "WHERE time >= '" +
             (start +
             timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ") +
             "' AND time <= '" +
             (start +
             timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ") +
             "'")
    uncontr = client.query(query)['uncontr']

    # Get the reference t=0
    first_t = uncontr.iloc[0].name
    uncontr_t = uncontr.index
    uncontr_index = list(range(0, len(uncontr_t)))

    # Change index to integers and rename column
    opt_uncontr = uncontr.copy()
    opt_uncontr['index'] = uncontr_index
    opt_uncontr.set_index('index', drop=True, inplace=True)
    opt_uncontr.rename(columns={'uncontr': 'p'}, inplace=True)

    # Query order books
    try:
        query = ("select * from bbook " +
             "WHERE startby >= " +
             str(int((start +
             timedelta(minutes=5)).timestamp() * 1000)) +
             " AND endby <= " +
             str(int((start +
             timedelta(hours=24)).timestamp() * 1000)))
        bbook = client.query(query)['bbook']

        # Set startby and endby as integers
        opt_bbook = bbook.copy()
        opt_bbook['startby'] -= first_t.timestamp() * 1000
        opt_bbook['startby'] /= 60 * 1000 * 60 / TIMESTEP
        opt_bbook['endby'] -= first_t.timestamp() * 1000
        opt_bbook['endby'] /= 60 * 1000 * 60 / TIMESTEP
        opt_bbook['id'] = list(range(0, len(opt_bbook)))
        opt_bbook.set_index('id', drop=True, inplace=True)
    except:
        # No orders at the moment
        opt_bbook = pandas.DataFrame()

    try:
        query = ("select * from sbook " +
                 "WHERE startby >= " +
                 str(int((start +
                 timedelta(minutes=5)).timestamp() * 1000)) +
                 " AND endby <= " +
                 str(int((start +
                 timedelta(hours=24)).timestamp() * 1000)))
        sbook = client.query(query)['sbook']

        # Set startby and endby as integers
        opt_sbook = sbook.copy()
        opt_sbook['startby'] -= first_t.timestamp() * 1000
        opt_sbook['startby'] /= 60 * 1000 * 60 / TIMESTEP
        opt_sbook['endby'] -= first_t.timestamp() * 1000
        opt_sbook['endby'] /= 60 * 1000 * 60 / TIMESTEP
        opt_sbook['id'] = list(range(0, len(opt_sbook)))
        opt_sbook.set_index('id', drop=True, inplace=True)
    except:
        # No orders at the moment
        opt_sbook = pandas.DataFrame()


    try:
        query = ("select * from dbook " +
                 "WHERE startby >= " +
                 str(int((start +
                 timedelta(minutes=5)).timestamp() * 1000)) +
                 " AND endby <= " +
                 str(int((start +
                 timedelta(hours=24)).timestamp() * 1000)))
        dbook = client.query(query)['dbook']

        opt_dbook = dbook.copy()
        opt_dbook['startby'] -= first_t.timestamp() * 1000
        opt_dbook['startby'] /= 60 * 1000 * 60 / TIMESTEP
        opt_dbook['endby'] -= first_t.timestamp() * 1000
        opt_dbook['endby'] /= 60 * 1000 * 60 / TIMESTEP
        opt_dbook['id'] = list(range(0, len(opt_dbook)))
        opt_dbook.set_index('id', drop=True, inplace=True)
        # Turn profile_kw from str to floats
        opt_dbook['profile_kw'] = opt_dbook['profile_kw'].apply(
            lambda x: [float(v) for v in
                       x[1:][:-1].replace(" ", "").split(',')])
    except:
        # No orders at the moment
        opt_dbook = pandas.DataFrame()

    # Run the optimization
    tic = datetime.now()
    result = maximize_self_consumption(
            opt_uncontr,
            opt_bbook,
            opt_sbook,
            opt_dbook,
            timestep=1/TIMESTEP,
            solver='glpk',
            verbose=False, timelimit=60)
    logger.info('GLPK time elapsed (hh:mm:ss.ms) {}'.format(
        datetime.now() - tic))

    # Save results back to influxDB (and remove previous schedule)
    total = uncontr.copy()
    total.rename(columns={'uncontr': 'contr'}, inplace=True)
    total['contr'] += result['demand_controllable']
    client.write_points(total, 'contr')

    client.drop_measurement('bschedule')
    if result['batteryin'] is not None:
        bschedule = (result['batteryin'] - result['batteryout']).copy()
        bschedule['index'] = uncontr_t
        bschedule.set_index('index', drop=True, inplace=True)
        bschedule.rename_axis(None, inplace=True)
        client.write_points(bschedule, 'bschedule')

    client.drop_measurement('sschedule')
    if result['demandshape'] is not None:
        sschedule = result['demandshape'].copy()
        sschedule['index'] = uncontr_t
        sschedule.set_index('index', drop=True, inplace=True)
        sschedule.rename_axis(None, inplace=True)
        client.write_points(sschedule, 'sschedule')

    client.drop_measurement('dschedule')
    if result['demanddeferr'] is not None:
        dschedule = result['demanddeferr'].copy()
        dschedule['index'] = uncontr_t
        dschedule.set_index('index', drop=True, inplace=True)
        dschedule.rename_axis(None, inplace=True)
        client.write_points(dschedule, 'dschedule')

    # Close DB connection
    client.close()
