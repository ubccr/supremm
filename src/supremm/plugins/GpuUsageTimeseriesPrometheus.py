#!/usr/bin/env python
""" Block usage timerseries plugin - https://github.com/NVIDIA/gpu-monitoring-tools"""

from supremm.plugin import PrometheusTimeseriesNamePlugin
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError
import numpy
from collections import OrderedDict

class GpuUsageTimeseriesPrometheus(PrometheusTimeseriesNamePlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "gpu_usage")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'util': {
            'metric': 'DCGM_FI_DEV_GPU_UTIL{{instance=~"^{node}.+"}}',
            'timeseries_name': 'gpu{gpu}',
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
            data = self.query_range(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                labels = r.get('metric', {})
                name = timeseries_name.format(**labels)
                if str(idx) not in self._devicedata:
                    self._devicedata[str(idx)] = TimeseriesAccumulator(self._job.nodecount, self._job.walltime)
                if name not in self._names.values():
                    self._names[str(idx)] = name
                for v in r.get('values', []):
                    value = float(v[1])
                    if v[0] not in timeseries:
                        timeseries[v[0]] = []
                    timeseries[v[0]].append(value)
                    self._devicedata[str(idx)].adddata(mdata.nodeindex, v[0], value)
                idx += 1
        for t, v in timeseries.items():
            avg_usage = numpy.mean(v)
            self._data.adddata(mdata.nodeindex, t, avg_usage)
        return True
