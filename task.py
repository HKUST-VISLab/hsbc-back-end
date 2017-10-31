"""
Tasks for all tasks
"""

from src.weather_AQI_fetcher.weather_data_helper import WeatherFetcher as WF
from src.weather_AQI_fetcher.air_quality_fetch_helper import fetch_and_store_air_quality
from src.tsm_fetcher.tsm_fetcher_helper import TSMFetcher as TF
from src.utils import Logger, task_thread
import time

TSM_INTERVAL = 2 * 60
CURRENT_INTERVAL = 200
FORECAST_INTERVAL = 3600*3
AQ_INTERVAL = 1200

TOTAL_RUNNING_TIME = 3600*24*7

if __name__ == '__main__':
    tsm = TF()

    tasks = []
    tasks.append(task_thread(WF.fetch_and_store_weather_data, CURRENT_INTERVAL, TOTAL_RUNNING_TIME, False))
    tasks.append(task_thread(WF.fetch_and_store_weather_data, FORECAST_INTERVAL, TOTAL_RUNNING_TIME, True))
    tasks.append(task_thread(tsm.fetch_and_store, TSM_INTERVAL, TOTAL_RUNNING_TIME, False))
    tasks.append(task_thread(fetch_and_store_air_quality, AQ_INTERVAL, TOTAL_RUNNING_TIME ))
    for task in tasks:
        task.start()
    time.sleep(TOTAL_RUNNING_TIME)
    for task in tasks:
        task.join(0.1)
