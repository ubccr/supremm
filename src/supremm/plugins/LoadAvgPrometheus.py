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
                    ts = int(v[0])
                    if ts <= (mdata.start + self.start_trim) or ts >= (mdata.end - self.end_trim):
                        continue
                    value = float(v[1])
                    self._data[mdata.nodename].append(value)
        return True

    def results(self):

        meanval = []
        maxval = []
        meanvalpercore = []
        maxvalpercore = []

        hinv = self._job.getdata('hinv')

        for nodename, loaddata in self._data.iteritems():
            if loaddata.count() > 0:
                meanval.append(loaddata.mean())
                maxval.append(loaddata.max)

                if nodename in hinv:
                    if 'error' in hinv[nodename]:
                        continue
                    meanvalpercore.append(loaddata.mean() / hinv[nodename]['cores'])
                    maxvalpercore.append(loaddata.max / hinv[nodename]['cores'])

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

