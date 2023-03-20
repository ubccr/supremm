import logging
from abc import ABC, abstractmethod

from supremm.errors import ProcessingError
from supremm.proc_common import instantiatePlugins

class Datasource(ABC):
    """ Definition of the Datasource API """

    def __init__(self, preprocs, plugins):
        self._allpreprocs = preprocs
        self._allplugins = plugins

    @property
    def allpreprocs(self):
        return self._allpreprocs

    @allpreprocs.setter
    def allpreprocs(self, preprocs):
        self._allpreprocs = preprocs

    @property
    def allplugins(self):
        return self._allplugins

    @allplugins.setter
    def allplugins(self, plugins):
        self._allplugins = plugins

    @abstractmethod
    def presummarize(self, job, config, resconf, opts):

        jobmeta = JobMeta()

        # Filter jobs by options
        if job.nodecount > 1 and opts['min_parallel_duration'] != None and job.walltime < opts['min_parallel_duration']:
            jobmeta.result = 1
            jobmeta.mdata["skipped_parallel_too_short"] = True
            jobmeta.error = ProcessingError.PARALLEL_TOO_SHORT
            # Was "skipped"
            jobmeta.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_parallel_too_short", job.job_id)
        elif opts['min_duration'] != None and job.walltime < opts['min_duration']:
            jobmeta.result = 1
            jobmeta.mdata["skipped_too_short"] = True
            jobmeta.error = ProcessingError.TIME_TOO_SHORT
            jobmeta.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_too_short", job.job_id)
        elif job.nodecount < 1:
            jobmeta.result = 1
            jobmeta.mdata["skipped_invalid_nodecount"] = True
            jobmeta.error = ProcessingError.INVALID_NODECOUNT
            jobmeta.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_invalid_nodecount", job.job_id)
        elif opts['max_nodes'] > 0 and job.nodecount > opts['max_nodes']:
            jobmeta.result = 1
            jobmeta.mdata["skipped_job_too_big"] = True
            jobmeta.error = ProcessingError.JOB_TOO_BIG
            jobmeta.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_job_too_big", job.job_id)
        elif opts['max_nodetime'] != None and (job.nodecount * job.walltime) > opts['max_nodetime']:
            jobmeta.result = 1
            jobmeta.mdata["skipped_job_nodehours"] = True
            jobmeta.error = ProcessingError.JOB_TOO_MANY_NODEHOURS
            jobmeta.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_job_too_big (node time)", job.job_id)
        elif opts['max_duration'] > 0 and job.walltime >= opts['max_duration']:
            jobmeta.result = 1
            jobmeta.mdata["skipped_too_long"] = True
            jobmeta.error = ProcessingError.TIME_TOO_LONG
            jobmeta.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_too_long", job.job_id)

        return jobmeta

    @abstractmethod
    def summarizejob(self, job, jobmeta, config, opts):
        # All datasources instantiate plugins/preprocs
        preprocessors = instantiatePlugins(self.allpreprocs, job)
        analytics = instantiatePlugins(self.allplugins, job)
        return preprocessors, analytics

    @abstractmethod
    def cleanup(self, job, opts):
        pass


class JobMeta():
    """ Container class for a job's metadata """

    def __init__():
        self.mdata = {}
        self.result = 0
        self.error = None
        self.missingnodes = 0

    @property
    def mdata(self):
        return self._mdata

    @mdata.setter
    def mdata(self, md):
        self._mdata = md

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, r):
        self._result = r

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, e):
        self._error = e

    @property
    def missingnodes(self):
        return self._missingnodes

    @missingnodes.setter
    def missingnodes(self, mn):
        self._missingnodes = mn
