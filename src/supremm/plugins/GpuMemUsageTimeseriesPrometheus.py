#!/usr/bin/env python
""" Block usage timerseries plugin - https://github.com/NVIDIA/gpu-monitoring-tools"""

from supremm.plugin import PrometheusTimeseriesNamePlugin
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError
import numpy
from collections import OrderedDict

class GpuMemUsageTimeseriesPrometheus(PrometheusTimeseriesNamePlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "gpu_mem_usage")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'memused': {
            'metrics': [
                '(DCGM_FI_DEV_FB_USED{{instance=~"^{node}.+"}} / 1024) * ON({host_label},gpu) {job_gpu_info}{{instance=~"^{node}.+",jobid="{jobid}"}}',
                '(DCGM_FI_DEV_FB_USED{{instance=~"^{node}.+"}} / 1024) * ON(gpu) {job_gpu_info}{{instance=~"^{node}.+",jobid="{jobid}"}}',
                '(DCGM_FI_DEV_FB_USED{{instance=~"^{node}.+"}} / 1024)',
            ],
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
            results = []
            for query_metric in metric['metrics']:
                if len(results) > 0:
                    continue
                query = query_metric.format(node=mdata.nodename, jobid=self._job.job_id, rate=self.rate, host_label=self.host_label, job_gpu_info=self.job_gpu_info)
                data = self.query_range(query, mdata.start, mdata.end)
                if data is None:
                    self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                    return None
                results = data.get('data', {}).get('result', [])
            for r in results:
                labels = r.get('metric', {})
                name = timeseries_name.format(**labels)
                if str(idx) not in self._devicedata:
                    self._devicedata[str(idx)] = TimeseriesAccumulator(self._job.nodecount, self._job.walltime)
                if name not in self._names.values():
                    self._names[str(idx)] = name
                for v in r.get('values', []):
                    ts = int(v[0])
                    if ts <= (mdata.start + self.start_trim) or ts >= (mdata.end - self.end_trim):
                        continue
                    value = float(v[1])
                    if v[0] not in timeseries:
                        timeseries[v[0]] = 0
                    timeseries[v[0]] += value
                    self._devicedata[str(idx)].adddata(mdata.nodeindex, v[0], value)
                idx += 1
        for t, v in timeseries.items():
            self._data.adddata(mdata.nodeindex, t, v)
        return True
