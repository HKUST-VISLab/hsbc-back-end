
from src.config import Config
from src.preprocess.tasks import run_tasks


def seed():
    Config.db_seed()

if __name__ == '__main__':
    seed()
    print('collection created')
    run_tasks()
