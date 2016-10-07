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
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["perfevent.hwcounters.CPU_CLK_UNHALTED.value"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(BlueWatersCpu, self).__init__(job)
        self._data = {}
        self._last = {}
        self._firsttime = {}
        self._lasttime = {}
        self._totalcores = 0
        self._error = None

    def process(self, nodemeta, timestamp, data, description):

        if self._job.getdata('perf')['active'] != True:
            self._error = ProcessingError.RAW_COUNTER_UNAVAILABLE
            return False

        if data[0].size == 0:
            return False

        ndata = numpy.array(data)

        if nodemeta.nodename not in self._last:
            self._last[nodemeta.nodename] = ndata
            self._firsttime[nodemeta.nodename] = timestamp
            self._lasttime[nodemeta.nodename] = timestamp
            return True

        if nodemeta.nodename not in self._data:
            # Only populate data for a host when we have at least 2 datapoints
            self._data[nodemeta.nodename] = numpy.zeros(ndata.shape)
            self._totalcores += data[0].size

        deltaV = (ndata - self._last[nodemeta.nodename]) % (2**48)
        deltaT = timestamp - self._lasttime[nodemeta.nodename]

        # Sane limit is 10 GHz ?
        if (deltaV/deltaT).any() > 10**9:
            self._error = ProcessingError.PMDA_RESTARTED_DURING_JOB
            return False

        self._data[nodemeta.nodename] += deltaV

        self._last[nodemeta.nodename] = ndata
        self._lasttime[nodemeta.nodename] = timestamp


        return True

    def results(self):

        if self._error != None:
            return {"error": self._error}
        
        nhosts = len(self._last)

        if nhosts < 1:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        ratios = numpy.empty(self._totalcores, numpy.double)

        coreindex = 0
        for host, data in self._data.iteritems():
            elapsed = self._lasttime[host] - self._firsttime[host]
            if elapsed < 1.0:
                return {"error": ProcessingError.JOB_TOO_SHORT}

            coresperhost = data.size
            ratios[coreindex:(coreindex+coresperhost)] = data / elapsed / 2.6e9
            coreindex += coresperhost

        # Compute the statistics for all cpus that had an average 
        effectivecpus = numpy.compress(ratios * 2.6e3 > CPU_ON_THRESHOLD_MHZ, ratios)

        return {"nodecpus": {"user": calculate_stats(ratios)}, "effectivecpus": {"user": calculate_stats(effectivecpus)}}

