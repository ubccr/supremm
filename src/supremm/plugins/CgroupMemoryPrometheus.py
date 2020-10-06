#!/usr/bin/env python
""" Memory usage plugin - https://github.com/treydock/cgroup_exporter"""

from supremm.plugin import PrometheusPlugin
from supremm.statistics import RollingStats, calculate_stats
from supremm.errors import ProcessingError

class CgroupMemoryPrometheus(PrometheusPlugin):
    """ Cgroup memory statistics for the job """

    name = property(lambda x: "process_memory")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        "usage": {
            'metric': 'cgroup_memory_used_bytes{{instance=~"^{node}.+"}} * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}}',
        },
        "limit": {
            'metric': 'cgroup_memory_total_bytes{{instance=~"^{node}.+"}} * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}}',
        },
        "usageratio": {
            'metric': """
            (cgroup_memory_used_bytes{{instance=~"^{node}.+"}} * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}}) / 
            (cgroup_memory_total_bytes{{instance=~"^{node}.+"}} * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}})
            """,
        }
    })

    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def process(self, mdata):
        self._data[mdata.nodeindex] = {}
        for metricname, metric in self.allmetrics.items():
            query = metric['metric'].format(node=mdata.nodename, jobid=self._job.job_id, rate=self.rate)
            data = self.query(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                for v in r.get('values', []):
                    value = float(v[1])
                    if metricname not in self._data[mdata.nodeindex]:
                        self._data[mdata.nodeindex][metricname] = RollingStats()
                    self._data[mdata.nodeindex][metricname].append(value)

        return True

    def results(self):
        if self._error != None:
            return {"error": self._error}
        if len(self._data) != self._job.nodecount:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        stats = {"usage": {"avg": [], "max": []}, "limit": [], "usageratio": {"avg": [], "max": []}}

        datapoints = 0

        for memdata in self._data.itervalues():
            for metric, values in memdata.items():
                if values.count() > 0:
                    datapoints += 1
                    if metric in ['usage', 'usageratio']:
                        stats[metric]['avg'].append(values.mean())
                        stats[metric]['max'].append(values.max)
                    else:
                        stats[metric].append(values.max)

        if datapoints == 0:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        result = {"usage": {}, "usageratio": {}}
        result['usage']['avg'] = calculate_stats(stats['usage']['avg'])
        result['usage']['max'] = calculate_stats(stats['usage']['max'])
        result['usageratio']['avg'] = calculate_stats(stats['usageratio']['avg'])
        result['usageratio']['max'] = calculate_stats(stats['usageratio']['max'])
        result['limit'] = calculate_stats(stats['limit'])

        return result
