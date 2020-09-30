#!/usr/bin/env python
""" Proc information pre-processor 
https://github.com/treydock/cgroup_exporter
https://github.com/prometheus/node_exporter
"""

from collections import Counter
from supremm.plugin import PrometheusPlugin
from supremm.errors import ProcessingError

class ProcPrometheus(PrometheusPlugin):
    """ Parse and analyse the proc information for a job. Supports parsing the cgroup information
        from SLRUM and PBS/Torque (if available).
    """

    name = property(lambda x: "proc")
    metric_system = property(lambda x: "prometheus")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: {
        'cpusallowed': {
            'metric': 'cgroup_cpu_info{{instance=~"^{node}.+"}} * on(cgroup, instance) group_left(jobid) cgroup_info{{instance=~"^{node}.+",jobid="{jobid}"}}',
        },
        'hostcpus': {
            'metric': 'count by(instance) (node_cpu_info{{instance=~"{node}.+"}})',
        }
    })

    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def __init__(self, job, config):
        super(ProcPrometheus, self).__init__(job, config)

        self.cpusallowed = None
        self.hostcpus = None
        self.hostname = None
        self.output = {"procDump": {"constrained": Counter(), "unconstrained": Counter()}, "cpusallowed": {}, "hostcpus": {}}

    def hoststart(self, hostname):
        self.hostname = hostname
        self.output['cpusallowed'][hostname] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}
        self.output['hostcpus'][hostname] = {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

    def hostend(self):
        if self.cpusallowed is not None:
            self.output['cpusallowed'][self.hostname] = self.cpusallowed
        if self.hostcpus is not None:
            self.output['hostcpus'][self.hostname] = self.hostcpus

        self.cpusallowed = None
        self.hostcpus = None
        self.hostname = None

        self._job.adddata(self.name, self.output)

    def process(self, mdata):
        for metricname, metric in self.allmetrics.items():
            query = metric['metric'].format(node=mdata.nodename, jobid=self._job.job_id, rate='5m')
            data = self.query(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                values = r.get('values', [])
                m = r.get('metric', {})
                if len(values) == 0:
                    continue
                if metricname == 'cpusallowed':
                    value = m.get('cpus', "").split(",")
                    self.cpusallowed = value
                elif metricname == 'hostcpus':
                    value = float(values[0][1])
                    self.hostcpus = value

        return True

    def results(self):
        if self._error != None:
            return {"error": self._error}

        result = {"constrained": [],
                  "unconstrained": [],
                  "cpusallowed": {}}

        for metric, values in self.output.items():
            result[metric] = {}
            for hostname, value in values.items():
                result[metric][hostname] = value
            
        return {'procDump': result}
