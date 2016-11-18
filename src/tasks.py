
from src.preprocess.weather_data_helper import WeatherFetcher as WF


def seed():
    WF.create_basic_collection()

if __name__ == '__main__':
    seed()
