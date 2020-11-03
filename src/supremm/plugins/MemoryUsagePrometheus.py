#!/usr/bin/env python
""" Memory usage plugin - https://github.com/prometheus/node_exporter"""

from supremm.plugin import PrometheusPlugin
from supremm.statistics import RollingStats, calculate_stats
from supremm.errors import ProcessingError

class MemoryUsagePrometheus(PrometheusPlugin):
    """ Compute the overall memory usage for a job """

    name = property(lambda x: "memory")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'used': {
            'metric': 'node_memory_MemTotal_bytes{{instance=~"^{node}.+"}} - node_memory_MemFree_bytes{{instance=~"^{node}.+"}}',
        },
        'used_minus_cache': {
            'metric': """node_memory_MemTotal_bytes{{instance=~"^{node}.+"}}
                - node_memory_MemFree_bytes{{instance=~"^{node}.+"}}
                - node_memory_Cached_bytes{{instance=~"^{node}.+"}}
                - node_memory_Slab_bytes{{instance=~"^{node}.+"}}""",
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def process(self, mdata):
        """ Memory statistics are the aritmetic mean of all values except the
            first and last rather than storing all of the meory measurements for
            the job, we use the RollingStats() class to keep track of the mean
            values. Since we don't know which data point is the last one, we update
            the RollingStats with the value from the previous timestep at each timestep.  
        """
        self._data[mdata.nodename] = {
            'used': RollingStats(),
            'used_minus_cache': RollingStats(),
        }
        for metricname, metric in self.allmetrics.items():
            query = metric['metric'].format(node=mdata.nodename, rate=self.rate)
            data = self.query_range(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                for v in r.get('values', []):
                    value = float(v[1])
                    self._data[mdata.nodename][metricname].append(value)

        return True

    def results(self):
        hinv = self._job.getdata('hinv')
        memused = []
        memusedminus = []

        for nodename, memdata in self._data.iteritems():
            if nodename not in hinv:
                return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}
            if 'error' in hinv[nodename]:
                return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}
            if memdata['used'].count() > 0:
                memused.append(memdata['used'].mean() / hinv[nodename]['cores'])
            if memdata['used_minus_cache'].count() > 0:
                memusedminus.append(memdata['used_minus_cache'].mean() / hinv[nodename]['cores'])

        if len(memused) == 0:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        return {"used": calculate_stats(memused), "used_minus_cache": calculate_stats(memusedminus)}
