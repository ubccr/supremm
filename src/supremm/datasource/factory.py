from supremm.datasource.pcp.pcpdatasource import PCPDatasource


class DatasourceFactory():
    """ Datasource class helper """

    def __init__(self, preprocs, plugins, resconf):

        if resconf["datasource"] == "pcp":
            self._datasource = PCPDatasource(preprocs, plugins)
        #elif resconf["datasource"] == "prometheus":
        #    self._datasource = PromDatasource(preprocs, plugins, config, resconf, opts)

    def presummarize(self, job, config, resconf, opts):
        return self._datasource.presummarize(job, config, resconf, opts)

    def summarizejob(self, job, config, opts):
        return self._datasource.summarizejob(job, config, opts)

    def cleanup(self, job, opts):
        return self._datasource.cleanup(job, opts)
