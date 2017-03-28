#!/usr/bin/env python

from supremm.plugin import Plugin
from supremm.errors import ProcessingError
import numpy

class Catastrophe(Plugin):
    """ Catastrophe analytic. Algorithm originally developed by Bill Barth et al. for the
        tacc_stats project """

    name = property(lambda x: "catastrophe")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: [["perfevent.hwcounters.MEM_LOAD_RETIRED_L1D_HIT.value"],
                                          ["perfevent.hwcounters.L1D_REPLACEMENT.value"],
                                          ["perfevent.hwcounters.DATA_CACHE_MISSES_DC_MISS_STREAMING_STORE.value"]])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(Catastrophe, self).__init__(job)
        self._data = {}
        self._error = None

    def process(self, nodemeta, timestamp, data, description):

        if self._job.getdata('perf')['active'] != True:
            self._error = ProcessingError.RAW_COUNTER_UNAVAILABLE
            return False

        if len(data[0]) == 0:
            # Ignore datapoints where no data stored
            return True

        if nodemeta.nodename not in self._data:
            self._data[nodemeta.nodename] = {"x": [], "t": []}

        info = self._data[nodemeta.nodename]
        info['x'].append(1.0 * numpy.sum(data[0]))
        info['t'].append(timestamp)

        if len(info['x']) > 1:
            if numpy.any(info['x'][-1] - info['x'][-2] < 0.0):
                self._error = ProcessingError.PMDA_RESTARTED_DURING_JOB
                return False

        return True

    def results(self):

        if self._error:
            return {"error": self._error}

        if len(self._data) == 0:
            return {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

        vals = None

        for _, data in self._data.iteritems():

            start = 2
            end = len(data['x'])-2

            for i in xrange(start+1, end-1):

                a = (data['x'][i] - data['x'][start]) / (data['t'][i] - data['t'][start])
                b = (data['x'][end] - data['x'][i]) / (data['t'][end] - data['t'][i])
                vals = b/a if vals == None else min(vals, b/a)

        if vals == None:
            return {"error": ProcessingError.JOB_TOO_SHORT}

        return {"value": vals}
