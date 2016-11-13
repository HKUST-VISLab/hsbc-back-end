from . import DB as DB
from src.preprocess.weather_data_helper import create_basic_collection


def seed():
    create_basic_collection()

if __name__ == '__main__':
    seed()