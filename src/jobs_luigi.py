# -*- coding: utf-8 -*-

import os, subprocess
import luigi, luigi.postgres
import datetime
import pandas as pd
import numpy as np
import sys, tempfile
import time, luigi.s3, sqlalchemy

from dateutil.relativedelta import relativedelta
from luigi.contrib.external_program import ExternalProgramRunError, ExternalProgramRunContext
from luigi.contrib.external_program import logger as luigi_logger
from luigi.contrib.spark import SparkSubmitTask

from luigi.task import flatten
from luigi.mock import MockFile

from sqlalchemy.sql import text
from urllib.parse import urlparse

from luigi import configuration

output_folder = 'data/'


# ====================================================
# Helper functions
# ====================================================

def save_output(df, output, na_rep=''):
    with output.open('w') as out_file:
        df.to_csv(out_file, sep='\t', encoding='utf-8', index=None, na_rep=na_rep)
    return True


def produce_full_filename(filename, start_date, end_date, country, dt):
    filename = '{filename}_{dt}.csv'.format(
        filename=filename, dt=dt, start_date=start_date, end_date=end_date, country=country)
    path = output_folder + filename
    print("new path: {0}".format(path))
    return path


def remove_target(file_path):
    os.remove(file_path)
    print("file {0} deleted").format(file_path)
    return file_path


# ====================================================
# Job templates
# ====================================================

default_days = 7  # specify here default interval for template tasks


class DateDayParameter(luigi.DateHourParameter):
    """
        Parameter whose value is a :py:class:`~datetime.datetime` specified to the date.
    """

    date_format = '%Y-%m-%d'  # ISO 8601 is to use 'T'
    _timedelta = datetime.timedelta(hours=1)


class CustomListParameter(luigi.Parameter):
    def parse(self, arguments):
        return arguments.replace('(', '') \
            .replace(')', '') \
            .replace('[', '') \
            .replace(']', '') \
            .replace('\'', '') \
            .replace(' ', '').split(',')


class CustomDateMinuteParameter(luigi.DateMinuteParameter):
    date_format = "%Y-%m-%dT%H%M"

    def serialize(self, x):
        return x.strftime("%Y-%m-%dT%H%M")


class JobDaily(luigi.Task):
    """
    Template for tasks
    """

    dt = luigi.Parameter(default=str(datetime.datetime.now()).replace(" ", ""))
    start_date = luigi.DateParameter(default=datetime.datetime.today() - datetime.timedelta(days=default_days))
    end_date = luigi.DateParameter(default=datetime.datetime.today())

    def output(self):
        filename = self.filename
        path = produce_full_filename(filename, self.start_date, self.end_date, self.country, self.dt)
        return luigi.LocalTarget(path)


class DataToDB(luigi.postgres.CopyToTable):
    """
    template job class storing db info
    save data from file to the analytics db
    
    start_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=default_days))
    end_date = luigi.DateParameter(default=datetime.date.today())

    dt = luigi.Parameter(default=str(datetime.datetime.now()).replace(" ", ""))
    # using parameter to make sure I can rerun job on spot xx try to remove this

    country = luigi.Parameter(default=['SG', 'TW', 'CN', 'CN-city', 'HK', 'KR'])

    analytics = urlparse(con_strings['analytics'])
    host = analytics.hostname
    database = analytics.path[1:]
    user = analytics.username
    password = analytics.password
    reflect = True
    column_delete = 'date'
    null_values = (None, 'nan', '', "")

    # If database table is already created, then the schema can be loaded
    # by setting the reflect flag to True
    # column_separator = '\t' # can change that to comma

    def run(self):
        if self.column_delete != "":
            for country in self.country:
                delete_rows(self.table, self.column_delete, self.start_date, self.end_date, country)
        super().run()

    def rows(self):
        with self.input().open('r') as fobj:
            for line in fobj.readlines()[1:]:
                yield line.strip('\n').split('\t')

    @property
    def columns(self):
        return self.input().open().readlines()[0].split('\t')

    """
    pass


class DeleteDB(luigi.Task):
    start_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=default_days))
    end_date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        return MockFile("DeleteMock", mirror_on_stderr=True)


# ====================================================
# Jobs - main part to add/modify tasks
# ====================================================

class task_to_run(JobDaily):
    start_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(days=30))
    end_date = luigi.DateParameter(default=datetime.date.today())

    filename = 'CS_analysis_etl'

    def run(self):
        pass



# ====================================================


if __name__ == '__main__':
    luigi.run()
