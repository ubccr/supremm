import logging

from supremm.datasource.pcp.pcpdatasource import PCPDatasource
from supremm.datasource.prometheus.promdatasource import PromDatasource


class DatasourceFactory():
    """ Datasource class helper """

    def __init__(self, preprocs, plugins, resconf):

        if resconf["datasource"] == "pcp":
            self._datasource = PCPDatasource(preprocs, plugins)
        elif resconf["datasource"] == "prometheus":
            self._datasource = PromDatasource(preprocs, plugins, resconf)
        else:
            logging.error("Invalid datasource in configuration: %s", resconf["datasource"])

    def presummarize(self, job, config, resconf, opts):
        return self._datasource.presummarize(job, config, resconf, opts)

    def summarizejob(self, job, jobmeta, config, opts):
        return self._datasource.summarizejob(job, jobmeta, config, opts)

    def cleanup(self, opts, job):
        return self._datasource.cleanup(opts, job)
