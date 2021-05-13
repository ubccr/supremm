#!/usr/bin/env python
""" Energy usage plugin - https://github.com/NVIDIA/gpu-monitoring-tools"""

from supremm.plugin import PrometheusPlugin
from supremm.errors import ProcessingError

class GpuUsagePrometheus(PrometheusPlugin):
    """ Compute the power statistics for a job """

    name = property(lambda x: "gpu")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'memused': {
            'metrics': [
                '(DCGM_FI_DEV_FB_USED{{instance=~"^{node}.+"}} * 1024^2) * ON({host_label},gpu) {job_gpu_info}{{instance=~"^{node}.+",jobid="{jobid}"}}',
                '(DCGM_FI_DEV_FB_USED{{instance=~"^{node}.+"}} * 1024^2) * ON(gpu) {job_gpu_info}{{instance=~"^{node}.+",jobid="{jobid}"}}',
                'DCGM_FI_DEV_FB_USED{{instance=~"^{node}.+"}} * 1024^2',
            ],
            'indom': 'gpu{gpu}',
        },
        'util': {
            'metrics': [
                'DCGM_FI_DEV_GPU_UTIL{{instance=~"^{node}.+"}} * ON({host_label},gpu) {job_gpu_info}{{instance=~"^{node}.+",jobid="{jobid}"}}',
                'DCGM_FI_DEV_GPU_UTIL{{instance=~"^{node}.+"}} * ON(gpu) {job_gpu_info}{{instance=~"^{node}.+",jobid="{jobid}"}}',
                'DCGM_FI_DEV_GPU_UTIL{{instance=~"^{node}.+"}}',
            ],
            'indom': 'gpu{gpu}',
        },
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def process(self, mdata):
        for metricname, metric in self.allmetrics.items():
            indom_label = metric.get('indom', None)
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
                m = r.get('metric', {})
                device = indom_label.format(**m)
                if device not in self._data:
                    self._data[device] = {}
                if metricname not in self._data[device]:
                    self._data[device][metricname] = []
                for v in r.get('values', []):
                    ts = int(v[0])
                    if ts <= (mdata.start + self.start_trim) or ts >= (mdata.end - self.end_trim):
                        continue
                    value = float(v[1])
                    self._data[device][metricname].append(value)
        return True
