#!/usr/bin/env python
""" Load Average plugin - https://github.com/prometheus/node_exporter"""

from supremm.plugin import PrometheusPlugin
from supremm.statistics import RollingStats, calculate_stats
from supremm.errors import ProcessingError

class LoadAvgPrometheus(PrometheusPlugin):
    """ Process the load average metrics """

    name = property(lambda x: "load1")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'load': {
            'metric': 'node_load1{{instance=~"^{node}.+"}}'
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def process(self, mdata):
        """ Computes the mean and max values of the load average for each node
           optionally normalizes this data to be per core (if the core count is available)
        """
        self._data[mdata.nodename] = RollingStats()
        for metricname, metric in self.allmetrics.items():
            query = metric['metric'].format(node=mdata.nodename, rate=self.rate)
            data = self.query_range(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                for v in r.get('values', []):
                    value = float(v[1])
                    self._data[mdata.nodename].append(value)
        return True

    def results(self):

        meanval = []
        maxval = []
        meanvalpercore = []
        maxvalpercore = []

        hostcpus = self._job.getdata('proc')['hostcpus']

        for nodename, loaddata in self._data.iteritems():
            if loaddata.count() > 0:
                meanval.append(loaddata.mean())
                maxval.append(loaddata.max)

                if hostcpus is not None and nodename in hostcpus:
                    meanvalpercore.append(loaddata.mean() / hostcpus[nodename])
                    maxvalpercore.append(loaddata.max / hostcpus[nodename])

        if len(meanval) == 0:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        results = {
            "mean": calculate_stats(meanval),
            "max": calculate_stats(maxval)
        }

        if len(meanvalpercore) > 0:
            results['meanpercore'] = calculate_stats(meanvalpercore)
            results['maxpercore'] = calculate_stats(maxvalpercore)

        return results

