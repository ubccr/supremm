#!/usr/bin/env python
""" Memory usage plugin """

from supremm.plugin import Plugin
from supremm.statistics import RollingStats, calculate_stats
from supremm.errors import ProcessingError

class NodeMemoryUsage(Plugin):
    """ Compute the overall memory usage for a job """

    name = property(lambda x: "nodememory")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: [["mem.freemem", "mem.physmem"], ["mem.util.free", "hinv.physmem", "mem.util.cached"]])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(NodeMemoryUsage, self).__init__(job)
        self._data = {}

    def process(self, nodemeta, timestamp, data, description):
        """ Memory statistics are the aritmetic mean of all values except the
            first and last rather than storing all of the memory measurements for
            the job, we use the RollingStats() class to keep track of the mean
            values. Since we don't know which data point is the last one, we update
            the RollingStats with the value from the previous timestep at each timestep.
        """

        if nodemeta.nodeindex not in self._data:
            self._data[nodemeta.nodeindex] = {'freeval': None,
                                              'free': RollingStats(),
                                              'cached': None,
                                              'physmem': None}
            return True

        hdata = self._data[nodemeta.nodeindex]

        if hdata['freeval'] != None:
            hdata['free'].append(hdata['freeval'])

        if len(data[0]) > 0:
            hdata['freeval'] = data[0][0]

        if hdata['physmem'] == None and len(data[1]) > 0:
            hdata['physmem'] = data[1][0]
            if len(data) == 3:
                hdata['physmem'] *= 1024.0

        if len(data) == 3:
            if hdata['cached'] == None:
                hdata['cached'] = RollingStats()

            hdata['cached'].append(data[0][0] + data[2][0])

        return True

    def results(self):

        memused = []
        memusedminus = []
        maxmemused = []
        maxmemusedminus = []
        memfree = []
        maxmemfree = []
        physmem = []

        for hostidx, memdata in self._data.items():
            if memdata['free'].count() > 0:
                memfree.append(memdata['free'].mean())
                maxmemfree.append(memdata['free'].max)

                if memdata['physmem'] != None:
                    memused.append(memdata['physmem'] - memdata['free'].mean())
                    maxmemused.append(memdata['physmem'] - memdata['free'].min)
                    physmem.append(memdata['physmem'])

                    if memdata['cached'] != None:
                        memusedminus.append(memdata['physmem'] - memdata['cached'].mean())
                        maxmemusedminus.append(memdata['physmem'] - memdata['cached'].min)

        if len(memused) == 0:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        result = {"used": calculate_stats(memused),
                "maxused": calculate_stats(maxmemused),
                "free": calculate_stats(memfree),
                "physmem": calculate_stats(physmem),
                "maxfree": calculate_stats(maxmemfree)}

        if len(memusedminus) > 0:
            result['used_minus_cache'] = calculate_stats(memusedminus)
            result['maxused_minus_cache'] = calculate_stats(maxmemusedminus)

        return result
