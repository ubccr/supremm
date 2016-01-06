#!/usr/bin/env python
""" Memory usage plugin """

from supremm.plugin import Plugin
from supremm.statistics import RollingStats, calculate_stats
from supremm.errors import ProcessingError

class MemoryUsage(Plugin):
    """ Compute the overall memory usage for a job """

    name = property(lambda x: "memory")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["mem.numa.util.used", "mem.numa.util.filePages", "mem.numa.util.slab", "kernel.percpu.cpu.user"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(MemoryUsage, self).__init__(job)
        self._data = {}
        self._hostcpucounts = {}

    def process(self, nodemeta, timestamp, data, description):
        """ Memory statistics are the aritmetic mean of all values except the
            first and last rather than storing all of the meory measurements for
            the job, we use the RollingStats() class to keep track of the mean
            values. Since we don't know which data point is the last one, we update
            the RollingStats with the value from the previous timestep at each timestep.  
        """

        if nodemeta.nodeindex not in self._data:
            self._data[nodemeta.nodeindex] = {'usedval': None, 
                                              'used': RollingStats(), 
                                              'usedminusval': None, 
                                              'usedminus': RollingStats()}
            return True

        if nodemeta.nodeindex not in self._hostcpucounts and data[3].size > 0:
            self._hostcpucounts[nodemeta.nodeindex] = data[3].size

        hdata = self._data[nodemeta.nodeindex]

        if hdata['usedval'] != None:
            hdata['used'].append(hdata['usedval'])
            hdata['usedminus'].append(hdata['usedminusval'])
            
        hdata['usedval'] = sum(data[0])
        hdata['usedminusval'] = (sum(data[0]) - sum(data[1]) - sum(data[2]))

        return True

    def results(self):

        memused = []
        memusedminus = []

        for hostidx, memdata in self._data.iteritems():
            if hostidx not in self._hostcpucounts:
                return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}
            if memdata['used'].count() > 0:
                memused.append(memdata['used'].mean() / self._hostcpucounts[hostidx])
            if memdata['usedminus'].count() > 0:
                memusedminus.append(memdata['usedminus'].mean() / self._hostcpucounts[hostidx])

        if len(memused) == 0:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        return {"used": calculate_stats(memused), "used_minus_cache": calculate_stats(memusedminus)}
