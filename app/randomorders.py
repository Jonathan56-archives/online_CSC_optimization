from datetime import datetime, timedelta
import random
import pandas
import numpy

def random_battery_orderbook():
    """Batteries"""
    # Order book for battery
    start = datetime.now()
    end = datetime.now() + timedelta(hours=20)
    startby =  (start + random.random() *
                (end - start)).replace(second=0, microsecond=0)
    endby = (startby + random.random() *
             (end - startby)).replace(second=0, microsecond=0)

    data = {'startby': [startby.timestamp() * 1000],
            'endby': [endby.timestamp() * 1000],
            'min_kw': [numpy.random.randint(2, 10)],
            'max_kw': [numpy.random.randint(2, 10)],
            'max_kwh': [numpy.random.randint(10, 100)],
            'initial_kwh': [numpy.random.randint(30, 100)],
            'end_kwh': [numpy.random.randint(0, 100)],
            'eta': [numpy.random.randint(85, 100)]}

    # Make sure that initial energy is under the maximum allowable
    data['initial_kwh'] = numpy.minimum(data['initial_kwh'], data['max_kwh'])

    # The end state is to go back to the initially feasible state
    data['end_kwh'] = data['initial_kwh'].copy()

    # Create a dataframe to be saved to influxdb
    df = pandas.DataFrame(
        index=[datetime.now().replace(second=0, microsecond=0)],
        data=data)
    df['eta'] = df['eta'] / 100
    return df

def random_shapeable_orderbook():
    """Shapeables"""
    # Order book for Shapeable
    start = datetime.now()
    end = datetime.now() + timedelta(hours=20)
    startby =  (start + random.random() *
                (end - start)).replace(second=0, microsecond=0)
    endby = (startby + random.random() *
             (end - startby)).replace(second=0, microsecond=0)

    data = {'startby': [startby.timestamp() * 1000],
            'endby': [endby.timestamp() * 1000],
            'max_kw': [numpy.random.randint(2, 10)],
            'end_kwh': [min(numpy.random.randint(10, 100),
                            (endby - startby).total_seconds() / 3600)]}

    # Make sure that energy level is reachable by full charging during
    # the entire period
    data['end_kwh'] = min(
        data['end_kwh'][0],
        (endby - startby).total_seconds() / 3600 * data['max_kw'][0])

    df = pandas.DataFrame(
        index=[datetime.now().replace(second=0, microsecond=0)],
        data=data)
    return df

def random_deferrable_orderbook(timestep):
    """Deferrable"""
    # Optimization timestep
    start = datetime.now()
    end = datetime.now() + timedelta(hours=20)
    startby =  (start + random.random() *
                (end - start)).replace(second=0, microsecond=0)
    endby = (startby + random.random() *
             (end - startby)).replace(second=0, microsecond=0)

    data = {'startby': [startby.timestamp() * 1000],
            'endby': [endby.timestamp() * 1000],
            'duration': [],
            'profile_kw': []}

    # Duration is within 5min to an hour or under startby - endby
    duration = min(int((endby - startby).total_seconds() / (timestep * 60)),
                   numpy.random.randint(1, 6))
    data['duration'].append(duration)

    # Random power profile
    profile = numpy.random.randint(1, 10, size=(duration,))
    data['profile_kw'].append(list(profile))

    df = pandas.DataFrame(
        index=[datetime.now().replace(second=0, microsecond=0)],
        data=data)
    return df
