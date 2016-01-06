""" definition of the accounting API and implementations of some base classes that
    include common functions """

from abc import ABCMeta, abstractmethod

class Accounting(object):
    """ abstract base class describing the job accounting interface """
    __metaclass__ = ABCMeta

    PROCESS_VERSION = 1

    def __init__(self, resource_id, config, nthreads, threadidx):
        self._resource_id = resource_id
        self._config = config
        self._nthreads = nthreads
        self._threadidx = threadidx

    @abstractmethod
    def getbylocaljobid(self, localjobid):
        """ Yields one or more Jobs that match the localjobid """
        pass

    @abstractmethod
    def getbytimerange(self, start, end):
        """ Search for all jobs based on the time interval. Matches based on the end
        timestamp of the job """
        pass

    @abstractmethod
    def get(self, start, end):
        """ Yields all unprocessed jobs. Optionally specify a time interval to process"""
        pass

    @abstractmethod
    def markasdone(self, job, success, elapsedtime):
        """ log a job as being processed (either successfully or not) """
        pass

class ArchiveCache(object):
    """ abstract base class describing the job archive cache interface """
    __metaclass__ = ABCMeta

    def __init__(self, config):
        self._config = config

    @abstractmethod
    def insert(self, resource_id, hostname, filename, start, end, jobid):
        """ insert a record into the cache """
        pass

    @abstractmethod
    def postinsert(self):
        """ Must be called after insert.  """
        pass
