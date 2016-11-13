"""
Tasks for preprocessing
"""

from src import preprocess
from src.preprocess import weather_data_helper
from src.utils import Logger as Logger
import time

if __name__ == '__main__':
    tasks = []
    tasks.append(weather_data_helper.fetch_and_store_weather_data)
    logger = Logger("tasks.py", "tasks.log")
    logger.info("Tasks start")
    start_time = time.time()
    end_time = time.time()-start_time
    while(end_time < 3600):
        for task in tasks:
            task()
        logger.info("finishing a round of tasks.")
        time.sleep(10*60)
        end_time = time.time()-start_time
    logger.info("ending process.")
