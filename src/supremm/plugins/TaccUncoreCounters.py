#!/usr/bin/env python3
""" Socket level performance counter plugin """

from supremm.plugin import Plugin
from supremm.statistics import calculate_stats
from supremm.errors import ProcessingError
import numpy

TACC_NHM_METRICS = ["taccstats_perfevent.hwcounters.UNC_LLC_MISS_READ.value",
                    "taccstats_perfevent.hwcounters.UNC_LLC_MISS_WRITE.value"]

class TaccUncoreCounters(Plugin):
    """ Compute various uncore performance counter derived metrics """

    name = property(lambda x: "uncperf")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: TACC_NHM_METRICS)
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(TaccUncoreCounters, self).__init__(job)
        self._last = {}
        self._data = {}
        self._error = None

    def process(self, nodemeta, timestamp, data, description):
        ndata = numpy.array(data)

        if nodemeta.nodename not in self._last:
            self._last[nodemeta.nodename] = ndata
            self._data[nodemeta.nodename] = 0.0
            return True

        if ndata.shape == self._last[nodemeta.nodename].shape:
            self._data[nodemeta.nodename] += numpy.sum((ndata - self._last[nodemeta.nodename]) % 2**48)
            self._last[nodemeta.nodename] = ndata
        else:
            # Perf counters changed during the job
            self._error = ProcessingError.RAW_COUNTER_UNAVAILABLE
            return False

        return True

    def results(self):

        if self._error != None:
            return {"error": self._error}

        nhosts = len(self._data)

        if nhosts < 1:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        membw = numpy.zeros(nhosts)
        for hostindex, data in enumerate(self._data.values()):
            membw[hostindex] = data * 64.0

        results = {"membw": calculate_stats(membw)}
        return results
