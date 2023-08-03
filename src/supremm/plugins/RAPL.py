#!/usr/bin/env python
""" RAPL Power Counter Measurements """

from supremm.plugin import Plugin
from supremm.statistics import calculate_stats
from supremm.errors import ProcessingError
import numpy

class RAPL(Plugin):

    name = property(lambda x: "rapl")
    mode = property(lambda x: "firstlast")
    requiredMetrics = property(lambda x: [["perfevent.hwcounters.rapl__RAPL_ENERGY_PKG.value",
                                            "perfevent.hwcounters.rapl__RAPL_ENERGY_CORES.value",
                                            "perfevent.hwcounters.rapl__RAPL_ENERGY_DRAM.value"]])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    # Scaling factor to convert RAPL energy measurements to Joules
    scaleFactor = 2**(-32)

    def __init__(self, job):
        super(RAPL, self).__init__(job)
        self._first = {}
        self._data = {}
        self._error = None
        self._totalcores = 0

    def process(self, nodemeta, timestamp, data, description):

        ndata = numpy.array(data)

        if nodemeta.nodename not in self._first:
            self._first[nodemeta.nodename] = {
                'energy': ndata,
                'time' : timestamp
            }
            return True
        if ndata.shape == self._first[nodemeta.nodename]['energy'].shape:
            self._data[nodemeta.nodename] = {
                'energy': ndata - self._first[nodemeta.nodename]['energy'],
                'time': timestamp - self._first[nodemeta.nodename]['time']
            }

            self._totalcores += data[0].size
        else:
            self._error = ProcessingError.RAW_COUNTER_UNAVAILABLE
            return False

        return True

    def results(self):
        if self._error != None:
            return {"error": self._error}

        nhosts = len(self._data)

        if nhosts < 1:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        pkgArray = numpy.zeros(self._totalcores)
        coresArray = numpy.zeros(self._totalcores)
        dramArray = numpy.zeros(self._totalcores)

        for _, data in self._data.iteritems():
            pkgArray = numpy.multiply(data['energy'][0], self.scaleFactor)
            coresArray = numpy.multiply(data['energy'][1], self.scaleFactor)
            dramArray = numpy.multiply(data['energy'][2], self.scaleFactor)

            wallTime = data['time']

        # Return energy in Joules and power in Watts (J/s)
        return {
            "power":{
                "pkg": calculate_stats(pkgArray)['avg']/wallTime,
                "cores": calculate_stats(coresArray)['avg']/wallTime,
                "dram": calculate_stats(dramArray)['avg']/wallTime
            },
            "energy": {
                "pkg": calculate_stats(pkgArray),
                "cores": calculate_stats(coresArray),
                "dram": calculate_stats(dramArray)
            },
        }


