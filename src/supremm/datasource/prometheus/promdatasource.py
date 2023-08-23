import datetime
import logging
import re

from supremm.datasource.datasource import Datasource
from supremm.datasource.prometheus.prommapping import MappingManager
from supremm.datasource.prometheus.prominterface import PromClient
from supremm.datasource.prometheus.promsummarize import PromSummarize
from supremm.errors import ProcessingError

PROMETHEUS_STR = "prometheus"


class PromDatasource(Datasource):
    """ Instance of a Prometheus datasource class """

    def __init__(self, preprocs, plugins, resconf):
        super().__init__(preprocs, plugins)

        self._client = PromClient(resconf)
        self._mapping = MappingManager(self.client)

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, c):
        self._client = c

    @property
    def mapping(self):
        return self._mapping

    @mapping.setter
    def mapping(self, m):
        self._mapping = m

    def presummarize(self, job, conf, resconf, opts):
        jobmeta = super().presummarize(job, conf, resconf, opts)

        # Initialize client and test connection
        if not self.client and not self.mapping:
            self.client = PromClient(resconf)
            if not self.client.connection:
                jobmeta.result = 1
                jobmeta.mdata["skipped_no_prom_connection"] = True
                jobmeta.error = ProcessingError.PROMETHEUS_CONNECTION
                logging.info("Skipping %s, skipped_no_prom_connection", job.job_id)
                jobmeta.missingnodes = job.nodecount
                return
            self.mapping = MappingManager(self.client)

        return jobmeta

    def summarizejob(self, job, jobmeta, config, opts):
        # Instantiate preproc, plugins
        preprocessors, analytics = super().summarizejob(job, jobmeta, config, opts)

        s = PromSummarize(preprocessors, analytics, job, config, self.mapping, opts["fail_fast"], PROMETHEUS_STR)

        enough_nodes = False

        # missingnodes will always == nodecount if there is a Prometheus error
        if 0 == jobmeta.result or (job.nodecount !=0 and (jobmeta.missingnodes / job.nodecount < 0.05)):
            enough_nodes = True
            logging.info("Success for prometheus presummarize checks, job %s (%s/%s)", job.job_id, jobmeta.missingnodes, job.nodecount)
            s.process()
        elif jobmeta.error == None and job.nodecount != 0 and (jobmeta.missingnodes / job.nodecount >= 0.5):
            # Don't overwrite existing error
            # Don't have enough node data to even try summarization
            jobmeta.mdata["skipped_prom_error"] = True
            logging.info("Skipping %s, skipped_prom_error", job.job_id)
            jobmeta.error = ProcessingError.PROMETHEUS_CONNECTION

        if opts['tag'] != None:
            jobmeta.mdata['tag'] = opts['tag']

        if jobmeta.missingnodes > 0:
            jobmeta.mdata['missingnodes'] = jobmeta.missingnodes

        success = s.good_enough()

        if not success and enough_nodes:
            # All other "known" errors should already be handled above.
            jobmeta.mdata["skipped_summarization_error"] = True
            logging.info("Skipping %s, skipped_summarization_error", job.job_id)
            jobmeta.error = ProcessingError.SUMMARIZATION_ERROR

        force_success = False
        if not success:
            force_timeout = opts['force_timeout']
            if (datetime.datetime.now() - job.end_datetime) > datetime.timedelta(seconds=force_timeout):
                force_success = True

        return s, jobmeta.mdata, success or force_success, jobmeta.error

    def cleanup(self, opts, job):
        # Nothing to be done for Prometheus
        pass
