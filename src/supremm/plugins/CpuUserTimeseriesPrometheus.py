#!/usr/bin/env python
""" Timeseries generator module - https://github.com/prometheus/node_exporter"""

from supremm.plugin import PrometheusTimeseriesNamePlugin
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError
import numpy
from collections import OrderedDict

class CpuUserTimeseriesPrometheus(PrometheusTimeseriesNamePlugin):
    """ Generate the CPU usage as a timeseries data """

    name = property(lambda x: "cpuuser")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'cpu': {
            'metric': 'rate(node_cpu_seconds_total{{instance=~"^{node}.+",mode="user",cpu=~"{cpus}"}}[{rate}])'
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def process(self, mdata):
        timeseries = OrderedDict()
        cpusallowed = self._job.getdata('proc').get('cpusallowed', {})
        if mdata.nodeindex not in self._hostdata:
            self._hostdata[mdata.nodeindex] = 1
        idx = 0
        for metricname, metric in self.allmetrics.items():
            usercpus = cpusallowed.get(mdata.nodename, None)
            if usercpus is None:
                self._error = ProcessingError.INSUFFICIENT_DATA
                return False
            cpus_query = "^(%s)$" % '|'.join(usercpus)
            query = metric['metric'].format(node=mdata.nodename, jobid=self._job.job_id, rate=self.rate, cpus=cpus_query)
            data = self.query_range(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                cpu = r.get('metric', {}).get('cpu', None)
                if cpu is None:
                    continue
                cpuname = "cpu%s" % cpu
                if str(idx) not in self._devicedata:
                    self._devicedata[str(idx)] = TimeseriesAccumulator(self._job.nodecount, self._job.walltime)
                if cpuname not in self._names.values():
                    self._names[str(idx)] = cpuname
                for v in r.get('values', []):
                    ts = v[0]
                    value = float(v[1])
                    if ts not in timeseries:
                        timeseries[ts] = []
                    timeseries[ts].append(value)
                    self._devicedata[str(idx)].adddata(mdata.nodeindex, ts, value)
                idx += 1
        for ts, values in timeseries.items():
            value = numpy.mean(values)
            self._data.adddata(mdata.nodeindex, ts, value)

        return True
