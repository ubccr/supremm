import logging
from abc import ABC, abstractmethod

from supremm.errors import ProcessingError
from supremm.proc_common import instantiatePlugins

class Datasource(ABC):
    """ Definition of the Datasource API """

    def __init__(self, preprocs, plugins):
        self._allpreprocs = preprocs
        self._allplugins = plugins

        self._mdata = {}
        self._result = 0
        self._error = None
        self._missingnodes = 0

        # PCP: configure archive_out_dir and subdir_out_format
        # 	args - archive_out_dir, subdir_out_format
        # Prometheus: initialize client, mapping
        #	args - prom_url

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

    @abstractmethod
    def presummarize(self, job, config, resconf, opts):

        # Both: Filter jobs by options
        if job.nodecount > 1 and opts['min_parallel_duration'] != None and job.walltime < opts['min_parallel_duration']:
            self.result = 1
            self.mdata["skipped_parallel_too_short"] = True
            self.error = ProcessingError.PARALLEL_TOO_SHORT
            # Was "skipped"
            self.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_parallel_too_short", job.job_id)
        elif opts['min_duration'] != None and job.walltime < opts['min_duration']:
            self.result = 1
            self.mdata["skipped_too_short"] = True
            self.error = ProcessingError.TIME_TOO_SHORT
            self.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_too_short", job.job_id)
        elif job.nodecount < 1:
            self.result = 1
            self.mdata["skipped_invalid_nodecount"] = True
            self.error = ProcessingError.INVALID_NODECOUNT
            self.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_invalid_nodecount", job.job_id)
        elif opts['max_nodes'] > 0 and job.nodecount > opts['max_nodes']:
            self.result = 1
            self.mdata["skipped_job_too_big"] = True
            self.error = ProcessingError.JOB_TOO_BIG
            self.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_job_too_big", job.job_id)
        elif opts['max_nodetime'] != None and (job.nodecount * job.walltime) > opts['max_nodetime']:
            self.result = 1
            self.mdata["skipped_job_nodehours"] = True
            self.error = ProcessingError.JOB_TOO_MANY_NODEHOURS
            self.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_job_too_big (node time)", job.job_id)
        elif opts['max_duration'] > 0 and job.walltime >= opts['max_duration']:
            self.result = 1
            self.mdata["skipped_too_long"] = True
            self.error = ProcessingError.TIME_TOO_LONG
            self.missingnodes = job.nodecount
            logging.info("Skipping %s, skipped_too_long", job.job_id)

    @abstractmethod
    def summarizejob(self, job):
        # All datasources instantiate plugins/preprocs
        preprocessors = instantiatePlugins(self.allpreprocs, job)
        analytics = instantiatePlugins(self.allplugins, job)
        return preprocessors, analytics

    def postsummarize(self):
        # Right now this does the same thing for both datasources
        pass

    @abstractmethod
    def cleanup(self, job, opts):
        pass
