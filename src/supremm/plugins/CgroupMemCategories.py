#!/usr/bin/env python
""" Memory usage catogorization plugin """

import re
import numpy as np
from supremm.plugin import Plugin
from supremm.errors import ProcessingError, NotApplicableError

class CgroupMemCategories(Plugin):
    """ Cgroup memory categorization plugin """

    name = property(lambda x: "process_memory_categories")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["cgroup.memory.usage"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    MIN_DATAPOINTS = 5

    def __init__(self, job):
        super(CgroupMemCategories, self).__init__(job)
        self._data = {}
        self._hostcounts = {}
        if job.acct['resource_manager'] == 'pbs':
            self._expectedcgroup = "/torque/{0}".format(job.job_id)
        elif job.acct['resource_manager'] == 'slurm':
            self._expectedcgroup = "/slurm/uid_{0}/job_{1}".format(job.acct['uid'], job.job_id)
        else:
            raise NotApplicableError

    def process(self, nodemeta, timestamp, data, description):
        """ Categorize a job based on its Cgroup memory usage """

        if len(data[0]) == 0:
            return True

        if nodemeta.nodeindex not in self._data:
            self._data[nodemeta.nodeindex] = []
            self._hostcounts[nodemeta.nodeindex] = {"present": 0, "missing": 0}
            # First data point for the node is ignored
            return True

        try:
            dataidx = None
            for idx, desc in enumerate(description[0][1]):
                if re.match(r"^" + re.escape(self._expectedcgroup) + r"($|\.)", desc):
                    dataidx = idx
                    break
            # No cgroup info at this datapoint
            if dataidx is None:
                return True
            for i in xrange(len(self.requiredMetrics)):
                if len(data[i]) < dataidx:
                    # Skip timesteps with incomplete information
                    raise ValueError

            self._hostcounts[nodemeta.nodeindex]["present"] += 1
        except ValueError:
            self._hostcounts[nodemeta.nodeindex]["missing"] += 1
            # No cgroup info at this datapoint
            return True

        self._data[nodemeta.nodeindex].append(data[0][dataidx])

        return True

    def results(self):
        if len(self._data) != self._job.nodecount:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        for hoststat in self._hostcounts.itervalues():
            if hoststat['missing'] > hoststat['present']:
                return {"error": ProcessingError.CPUSET_UNKNOWN}

        if len(self._data[0]) < self.MIN_DATAPOINTS:
            return {"error": ProcessingError.INSUFFICIENT_DATA, "length": len(self._data[0])}

        # Classify the job's memory usage
        total = np.sum(list(self._data.itervalues()), 0)
        first, middle, last = np.array_split(total, 3)
        first, middle, last = np.median(first), np.median(middle), np.median(last)

        # Number of zeroes used for threshold. -1 for the leading digit, -2 for the .0
        zeroes = len(str(middle)) - 3
        threshold = int('1'+'0'*zeroes) / 2

        if abs(middle - first) <= threshold and abs(last - middle) <= threshold:
            category = "CONSTANT"
        elif first < middle < last:
            category = "INCREASING"
        elif first > middle > last:
            category = "DECREASING"
        else:
            category = "INCONSISTENT"

        return {"category": category}
