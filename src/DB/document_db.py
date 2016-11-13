"""
Base classes for document-oriented database
"""

import abc
from abc import abstractmethod
from abc import ABCMeta


class DocumentDB(metaclass=ABCMeta):
    """
    An abstract class of a document-oriented database
    """

    def __init__(self, db_name, host, port):
        pass
