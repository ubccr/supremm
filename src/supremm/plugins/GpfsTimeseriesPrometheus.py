#!/usr/bin/env python
"""https://github.com/treydock/gpfs_exporter"""

from supremm.plugin import PrometheusTimeseriesNamePlugin
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError
from collections import OrderedDict

class GpfsTimeseriesPrometheus(PrometheusTimeseriesNamePlugin):
    """ Collect GPFS metrics from Prometheus """

    name = property(lambda x: "gpfs")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "read_bytes": {
            'metric': 'rate(gpfs_perf_read_bytes{{instance=~"^{node}.+"}}[{rate}])',
            'timeseries_name': 'read',
        },
        "write_bytes": {
            'metric': 'rate(gpfs_perf_write_bytes{{instance=~"^{node}.+"}}[{rate}])',
            'timeseries_name': 'write',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def process(self, mdata):
        timeseries = OrderedDict()
        idx = 0
        if mdata.nodeindex not in self._hostdata:
            self._hostdata[mdata.nodeindex] = 1
        for metricname, metric in self.allmetrics.items():
            timeseries_name = metric['timeseries_name']
            query = metric['metric'].format(node=mdata.nodename, jobid=self._job.job_id, rate=self.rate)
            data = self.query(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                fs = r.get('metric', {}).get('fs', None)
                if fs is None:
                    self._error = ProcessingError.INSUFFICIENT_DATA
                    return False
                if str(idx) not in self._devicedata:
                    self._devicedata[str(idx)] = TimeseriesAccumulator(self._job.nodecount, self._job.walltime)
                name = "%s-%s" % (fs, timeseries_name)
                if name not in self._names.values():
                    self._names[str(idx)] = name
                for v in r.get('values', []):
                    value = float(v[1])
                    if v[0] not in timeseries:
                        timeseries[v[0]] = 0
                    timeseries[v[0]] += value
                    self._devicedata[str(idx)].adddata(mdata.nodeindex, v[0], value)
                idx += 1
        for t, v in timeseries.items():
            self._data.adddata(mdata.nodeindex, t, v)
        return True
