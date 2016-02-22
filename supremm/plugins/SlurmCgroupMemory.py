#!/usr/bin/env python
""" Memory usage plugin """

from supremm.plugin import Plugin
from supremm.statistics import RollingStats, calculate_stats
from supremm.errors import ProcessingError

class SlurmCgroupMemory(Plugin):
    """ Cgroup memory statistics for the job """

    name = property(lambda x: "process_memory")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["cgroup.memory.usage", "cgroup.memory.limit"])

    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(SlurmCgroupMemory, self).__init__(job)
        self._data = {}
        self._hostcounts = {}
        self._expectedcgroup = "/slurm/uid_{0}/job_{1}".format(job.acct['uid'], job.job_id)

    def process(self, nodemeta, timestamp, data, description):
        """ CGroup Memory statistics are the aritmetic mean of all values except the
            first. Rather than storing all of the meory measurements for
            the job, we use the RollingStats() class to keep track of the mean
            values.
        """

        if len(data[0]) == 0:
            return True

        if nodemeta.nodeindex not in self._data:
            self._data[nodemeta.nodeindex] = [RollingStats() for i in xrange(len(self.requiredMetrics) + 1)]
            self._hostcounts[nodemeta.nodeindex] = {"present": 0, "missing": 0}
            # First data point for the node is ignored
            return True

        try:
            dataidx = description[0][1].index(self._expectedcgroup)
            self._hostcounts[nodemeta.nodeindex]["present"] += 1
        except ValueError:
            self._hostcounts[nodemeta.nodeindex]["missing"] += 1
            # No cgroup info at this datapoint
            return True

        hdata = self._data[nodemeta.nodeindex]

        for i in xrange(len(self.requiredMetrics)):
            hdata[i].append(data[i][dataidx])

        if data[1][dataidx] > 0.0:
            hdata[2].append(1.0 * data[0][dataidx] / data[1][dataidx])
        else:
            hdata[2].append(0.0)

        return True

    def results(self):

        if len(self._data) != self._job.nodecount:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        for hoststat in self._hostcounts.itervalues():
            if hoststat['missing'] > hoststat['present']:
                return {"error": ProcessingError.CPUSET_UNKNOWN}

        stats = {"usage": {"avg": [], "max": []}, "limit": [], "usageratio": {"avg": [], "max": []}}

        datapoints = 0

        for memdata in self._data.itervalues():
            if memdata[0].count() > 0:
                datapoints += 1
                stats["usage"]["avg"].append(memdata[0].mean())
                stats["usage"]["max"].append(memdata[0].max)
                stats["limit"].append(memdata[1].max)
                stats["usageratio"]["avg"].append(memdata[2].mean())
                stats["usageratio"]["max"].append(memdata[2].max)

        if datapoints == 0:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        result = {"usage": {}, "usageratio": {}}
        result["usage"]["avg"] = calculate_stats(stats["usage"]["avg"])
        result["usage"]["max"] = calculate_stats(stats["usage"]["max"])
        result["limit"] = calculate_stats(stats["limit"])
        result["usageratio"]["avg"] = calculate_stats(stats["usageratio"]["avg"])
        result["usageratio"]["max"] = calculate_stats(stats["usageratio"]["max"])

        return result
