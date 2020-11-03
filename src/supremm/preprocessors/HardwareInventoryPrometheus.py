#!/usr/bin/env python
""" hardware inventory pre-processor - https://github.com/prometheus/node_exporter"""

from supremm.plugin import PrometheusPlugin
from supremm.statistics import calculate_stats
from supremm.errors import ProcessingError

class HardwareInventoryPrometheus(PrometheusPlugin):
    """ Parse and analyse hardware inventory information. Currently
        grabs the number of CPU cores for each host.
    """

    name = property(lambda x: "hinv")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'ncpus': {
            'metric': 'count by(instance) (node_cpu_info{{instance=~"^{node}.+"}})'
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def __init__(self, job, config):
        super(HardwareInventoryPrometheus, self).__init__(job, config)
        self.hostname = None
        self.corecount = None
        self.data = {}
        self.cores = []

    def hoststart(self, hostname):
        self.hostname = hostname
        self.data[hostname] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

    def process(self, mdata):
        for metricname, metric in self.allmetrics.items():
            query = metric['metric'].format(node=mdata.nodename)
            data = self.query(query, mdata.start)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                if metricname == 'ncpus':
                    value = r.get('value', [None, "0"])[1]
                    self.corecount = float(value)
        return True

    def hostend(self):
        if self.corecount is not None and self.corecount != 0:
            self.data[self.hostname] = {'cores': self.corecount}
            self.cores.append(self.corecount)

        self.corecount = None
        self.hostname = None

        self._job.adddata(self.name, self.data)

    def results(self):
        if self._error != None:
            return {"error": self._error}
        if len(self.cores) != self._job.nodecount:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        return {"cores": calculate_stats(self.cores)}

