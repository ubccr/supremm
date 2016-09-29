#!/usr/bin/env python
""" CPU Usage inferred from the cpu clock. """

from supremm.plugin import Plugin
from supremm.statistics import calculate_stats
from supremm.errors import ProcessingError
import numpy

# If the average clock ticks are above the threshold then the core
# is considered in use by the 'efffective cpus' algorithm.
CPU_ON_THRESHOLD_MHZ = 100

class BlueWatersCpu(Plugin):
    """ Estimate the cpu usage for a job by looking a the clock ticks. """

    name = property(lambda x: "cpu")
    mode = property(lambda x: "firstlast")
    requiredMetrics = property(lambda x: ["perfevent.hwcounters.CPU_CLK_UNHALTED.value"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(BlueWatersCpu, self).__init__(job)
        self._first = {}
        self._last = {}
        self._totalcores = 0

    def process(self, nodemeta, timestamp, data, description):

        if self._job.getdata('perf')['active'] != True:
            self._error = ProcessingError.RAW_COUNTER_UNAVAILABLE
            return False

        if data[0].size == 0:
            return False

        if nodemeta.nodename not in self._first:
            self._first[nodemeta.nodename] = (timestamp, data[0])
            return True

        self._last[nodemeta.nodename] = (timestamp, data[0])
        self._totalcores += data[0].size

        return True

    def results(self):

        nhosts = len(self._last)

        if nhosts < 1:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        ratios = numpy.empty(self._totalcores, numpy.double)

        coreindex = 0
        for host, last in self._last.iteritems():
            elapsed = last[0] - self._first[host][0]
            if elapsed < 1.0:
                return {"error": ProcessingError.JOB_TOO_SHORT}

            coresperhost = last[1].size
            ratios[coreindex:(coreindex+coresperhost)] = (last[1] - self._first[host][1]) / elapsed / 2.6e9
            coreindex += coresperhost

        # Compute the statistics for all cpus that had an average 
        effectivecpus = numpy.compress(ratios * 2.6e3 > CPU_ON_THRESHOLD_MHZ, ratios)

        return {"nodecpus": {"user": calculate_stats(ratios)}, "effectivecpus": {"user": calculate_stats(effectivecpus)}}

