import os
import logging
import copy
import json

from supremm.config import Config


class MappingManager():
    """ Helper class to manage the mappings between PCP metrics and Prometheus metrics """

    def __init__(self, client):
        self._mapping = MappingManager.load_mapping()
        self._client = client
        self._job = None

    def __str__(self):
        return str(self.mapping)

    @staticmethod
    def load_mapping():
        """
        Update mapping of available Prometheus metrics
        with corresponding PCP metric names.
        """
        # Load mapping
        fpath = Config.autodetectconfpath("mapping.json")
        if not fpath:
            logging.warning("No metric mapping file present.")
            return

        file = os.path.join(fpath, "mapping.json")
        with open(file, "r") as f:
            mapping = json.load(f)

        # Populate common query params, defaults
        params = mapping["common"]["params"]
        defaults = mapping["common"]["defaults"]

        for pcp, prom in mapping["metrics"].items():
            mmap = MappingManager.query_builder(params, defaults, prom)
            mapping["metrics"][pcp] = mmap

        logging.debug("Loaded metric mapping from {}".format(fpath))
        return mapping

    @staticmethod
    def query_builder(params, defaults, prom_metric):
        """ Build base queries from mapping configuration """

        # Metric, params, defaults
        p = [*params]
        d = copy.copy(defaults)
        for setting, arg in prom_metric.items():
            # Add params
            if setting == "params":
                p = [*p, *arg]

            # Add defaults
            elif setting == "defaults":
                d.update(arg)

        plabels = []
        for label in p:
           plabels.append("{}='{{}}'".format(label))
        plabels = ",".join(plabels)

        dlabels = []
        for label, default in d.items():
            dlabels.append("{}='{}'".format(label, default))
        dlabels = ",".join(dlabels)

        name = prom_metric["name"]
        in_fmt = "{0}{{{{{1},{2}}}}}".format(name, plabels, dlabels)
        groupby = prom_metric["groupby"]
        try:
            scaling = prom_metric["scaling"]
        except KeyError:
            scaling = ""

        try:
            out_fmt = prom_metric["out_fmt"]
        except KeyError:
            out_fmt = groupby

        return MetricMapping(name, in_fmt, out_fmt, groupby, scaling, p[1:]) 

    @property
    def mapping(self):
        """ Dictionary of mappings between a PCP metric and a MetricMapping """
        return self._mapping["metrics"]

    @property
    def client(self):
        """ Client used to query metadata """
        return self._client

    @property
    def currentjob(self):
        """ Current job being processed """
        return self._job

    @currentjob.setter
    def currentjob(self, job):
        self._job = job
        self.cgroup = None

    @property
    def start(self):
        """ Job's start """
        return self.currentjob.start_datetime.timestamp()

    @property
    def end(self):
        """ Job's end """
        return self.currentjob.end_datetime.timestamp()

    @property
    def cgroup(self):
        if self._cgroup is not None:
            return self._cgroup
        else:
            uid = self.currentjob.acct["uid"]
            jobid = self.currentjob.job_id
            self.cgroup = self._client.cgroup_info(uid, jobid, self.start, self.end)
            return self._cgroup

    @cgroup.setter
    def cgroup(self, cgroup):
        self._cgroup = cgroup

    def populate_queries(self, nodename):
        """ Format queries with nodenames and other parameters if necessary """

        for map in self.mapping.values():
            if not map.params:
                map.query = map.queryformat.format(nodename)
            else:
                args = [nodename]
                for arg in map.params:
                    if arg == "cgroup":
                        if self.cgroup:
                            args.append(self.cgroup)
                        else:
                            map.query = None

                if len(args) == 1:
                    # Cannot populate query
                    continue
                
                map.query = map.queryformat.format(*args)

    def getmetricstofetch(self, reqMetrics):
        """
        Recursively checks if a mapping is available from a given metrics list or list of lists.

        params: reqMetrics - list of metrics from preproc/plugin
        return: List of MetricMappings if mapping is present.
                False if mapping not present.
        """

        if isinstance(reqMetrics[0], list):
            for metriclist in reqMetrics:
                mapping = self.getmetricstofetch(metriclist)
                if mapping:
                    return mapping
            return False

        else:
            prommetrics = []
            for m in reqMetrics:
                if m in self.mapping.keys():
                    query = self.mapping[m].query
                    if not query:
                        logging.warning("Query not built for metric %s", m)
                        return False
                    else:
                        if False == self._client.ispresent(query, self.start, self.end):
                            logging.warning("No data available for metric %s", m)
                            return False

                        prommetrics.append(self.mapping[m])
                else:
                    logging.debug("Mapping unavailable for metric: %s", m)
                    return False

            return prommetrics


class MetricMapping():
    """
    Container class for mapping between PCP metrics and Prometheus metrics.
    """

    def __init__(self, name, in_format, out_format, groupby, scaling, params):
        self._name = name
        self._queryformat = in_format
        self._outformat = out_format
        self._groupby = groupby
        self._scaling = scaling
        self._params = params

        self._query = None

    def __str__(self):
        return self.query

    @property
    def name(self):
        return self._name

    @property
    def queryformat(self):
        """ Format string for metric query """
        return self._queryformat

    @property
    def outformat(self):
        """ Description output format (default is groupby) """
        return self._outformat

    @property
    def params(self):
        """ Additional parameters for a query """
        return self._params

    @property
    def groupby(self):
        """ Label name for a metric's unique identifier """
        return self._groupby

    @property
    def scaling(self):
        """ Operation that should be appended to query """
        return self._scaling

    @property
    def query(self):
        """ Query populated with necessary parameters """
        return self._query

    @query.setter
    def query(self, query):
        self._query = query

    def apply_range(self, start, end):
        """ Append range modifier for instant queries.
            This queries raw data from Prometheus.
        """
        range = end - start
        query = self.query + "[{}s]".format(int(range))
        return query
