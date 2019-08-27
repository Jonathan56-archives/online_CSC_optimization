from entsoe import EntsoePandasClient
from datetime import datetime, timedelta
import setting
import pandas
import schedule
import time
import requests
import json


def forecast():
    # Input
    start = datetime.now() - timedelta(hours=5)
    end = datetime.now() + timedelta(hours=24)
    country_code = 'FR'  # France

    # Print attempt time
    print('Job executed at ' + str(datetime.now()))
    print('Fetching forecast from ' + str(start)+ ' to ' + str(end))

    # Query data
    client = EntsoePandasClient(api_key=setting.key)
    ts = client.query_load_forecast(country_code, start=start,end=end)
    # We could concat historical data to have an accurate curve afterward
    #client.query_load(country_code, start=start - timedelta(hours=24),end=end)

    # Print sucess time range
    print('Retrieved forecast from ' + str(ts.index[0]) + ' to ' + str(ts.index[-1]))

    # Resample data (make sure time ends in 0 or 5)
    forecast = ts.resample('5T').interpolate()

    # Send data over to server
    url = 'http://fastapi/forecast'
    data = {'times': [d.strftime('%Y-%m-%dT%H:%M:%SZ')
                      for d in forecast.index],
            'values': list((forecast / 1000 - 30).tolist())}
    headers = {"Content-Type": "application/json"}
    response = requests.put(url, data=json.dumps(data), headers=headers)
    res = response.json()
    print('Forecast sent ' + str(res))

# Schedule forecast
forecast()  # do it once before
schedule.every().hour.do(forecast)

while True:
    schedule.run_pending()
    # Wake up every 10 min
    # to run what ever task
    time.sleep(10 * 60)
