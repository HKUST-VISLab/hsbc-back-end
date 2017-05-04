"""
Utils for this project
"""

import logging
import json
import codecs
import urllib
import time
import threading
from urllib import request, error
from urllib.error import HTTPError, URLError
import csv
import re
from datetime import datetime


class Logger:
    """
    A more convenient logger class
    """
    _log_format = logging.Formatter('[%(asctime)s] %(name)s [%(levelname)s]: %(message)s','%Y-%m-%d %H:%M:%S')

    def __init__(self, name, filename=None, formatter=_log_format, level=logging.INFO):
        """

        :param name: logger name
        :param filename: logging file, "mmddHHMM.log" by default
        :param formatter:
        :param level:
        """
        if filename is None:
            current_str = time.strftime("%m%d%H%M", time.localtime())
            filename = current_str+'.log'
        handler = logging.FileHandler(filename, 'w')
        handler.setFormatter(formatter)
        self.__logger = logging.getLogger(name)
        self.__logger.addHandler(handler)
        self.__logger.setLevel(level)

    def warn(self, message):
        self.__logger.warn(msg=message)

    def error(self, message):
        self.__logger.error(msg=message)

    def info(self, message):
        self.__logger.info(msg=message)

    def debug(self, message):
        self.__logger.debug(msg=message)

_logger = Logger(__name__)


def parse_json_file(fp_or_filename, encoding='utf-8'):
    _reader = codecs.getreader(encoding)
    if isinstance(fp_or_filename, str):
        return json.load(open(fp_or_filename, encoding=encoding))
    else:
        return json.load(_reader(fp_or_filename))


def parse_csv_file(fp_or_filename, has_title=False, has_header=False, encoding='utf-8'):
    fp = fp_or_filename
    if isinstance(fp, str):
        fp = open(fp, newline='', encoding=encoding)
    else:
        fp = codecs.iterdecode(fp, encoding)
    reader = csv.reader(fp, delimiter=',')
    data = []
    try:
        title = next(reader) if has_title else None
        header = next(reader) if has_header else None
        for row in reader:
            data.append(row)
    except csv.Error as e:
        _logger.error('CSV Loading error!')
        return str(e)
    return data


def camel2snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def safe_open_url(path):

    try:
        response = urllib.request.urlopen(path)
    except HTTPError as e:
        data = str(e.code)
        _logger.error('HTTPError = ' + data + '. Check grid_id!')
        return data
    except URLError as e:
        data = str(e.reason)
        _logger.error('URLError = ' + data)
        return data
    else:
        return response


def task_thread(func, sleep_time, stop_time, args = None):
    def task(func, args):
        start_time = time.time()
        i = 0
        while(time.time() - start_time < stop_time):
            i += 1
            if args == None:
                func()
            else:
                func(args)
            # For more accurate period control when time length is too large
            time.sleep(sleep_time*i + start_time - time.time())
        return 0
    return threading.Thread(target=task, args=(func, args))

def time_convert(tm_str, t_format):
    # print(tm_str)
    if not isinstance(tm_str, str):
        tm_str = str(tm_str)
    return str(datetime.strptime(tm_str, t_format))

if __name__ == '__main__':
    def _test_hello(name):
        print('Hello, '+name)

    p = task_thread(_test_hello, 5, 15, 'ming')
    p.start()
    print('hi')
    time.sleep(5)
    print(p, p.is_alive())
    time.sleep(5)
    print(p, p.is_alive())
    print('bye')
    p.join(2)
    print('bye')