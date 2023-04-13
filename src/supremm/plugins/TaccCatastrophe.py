#!/usr/bin/env python3

from supremm.plugin import Plugin
from supremm.errors import ProcessingError
from supremm.subsample import RangeConverter
import numpy

class TaccCatastrophe(Plugin):
    """ Catastrophe analytic. Algorithm originally developed by Bill Barth et al. for the
        tacc_stats project """

    name = property(lambda x: "catastrophe")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: [ ["taccstats_perfevent.hwcounters.MEM_LOAD_RETIRED_L1D_HIT.value"], ["taccstats_perfevent.hwcounters.L1D_REPLACEMENT.value"] ])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(TaccCatastrophe, self).__init__(job)
        self._data = {}
        self._values = {}

    def process(self, nodemeta, timestamp, data, description):

        if nodemeta.nodename not in self._data:
            self._data[nodemeta.nodename] = { "x": [], "t": [] }
            self._values[nodemeta.nodename] = RangeConverter(48, False)

        info = self._data[nodemeta.nodename]
        value = self._values[nodemeta.nodename].append(data)

        info['x'].append(1.0 * numpy.sum(value))
        info['t'].append(timestamp)

        return True

    def results(self):

        if len(self._data) == 0:
            return {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

        vals = None

        for host, data in self._data.items():
            x = data['x']
            t = data['t']

            start = 2
            end = len(data['x'])-2

            for i in range(start+1, end-1):

                a = (data['x'][i] - data['x'][start]) / (data['t'][i] - data['t'][start])
                b = (data['x'][end] - data['x'][i]) / (data['t'][end] - data['t'][i])
                vals = b/a if vals == None else min(vals, b/a)

        if vals == None:
            return {"error": ProcessingError.JOB_TOO_SHORT}

        return {"value": vals}
