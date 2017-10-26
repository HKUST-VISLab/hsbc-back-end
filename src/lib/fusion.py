from library import *
import time
if __name__ == '__main__':
    start_time = time.time()
    generate_aggregation_collection()
    end_time = time.time()
    print("Total time", end_time - start_time)