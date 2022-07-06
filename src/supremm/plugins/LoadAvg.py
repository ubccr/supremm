#!/usr/bin/env python3
""" Load Average plugin """

from supremm.plugin import Plugin
from supremm.statistics import RollingStats, calculate_stats
from supremm.errors import ProcessingError

class LoadAvg(Plugin):
    """ Process the load average metrics """

    name = property(lambda x: "load1")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["kernel.all.load"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(LoadAvg, self).__init__(job)
        self._data = {}

    def process(self, nodemeta, timestamp, data, description):
        """ Computes the mean and max values of the load average for each node
           optionally normalizes this data to be per core (if the core count is available)
        """

        if data[0].size < 1:
            return True

        if nodemeta.nodename not in self._data:
            self._data[nodemeta.nodename] = RollingStats()
            return True

        self._data[nodemeta.nodename].append(data[0][0])

        return True

    def results(self):

        meanval = []
        maxval = []
        meanvalpercore = []
        maxvalpercore = []

        hinv = self._job.getdata('hinv')

        for nodename, loaddata in self._data.items():
            if loaddata.count() > 0:
                meanval.append(loaddata.mean())
                maxval.append(loaddata.max)

                if hinv != None and nodename in hinv:
                    meanvalpercore.append(loaddata.mean() / hinv[nodename]['cores'])
                    maxvalpercore.append(loaddata.max / hinv[nodename]['cores'])

        if len(meanval) == 0:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        results = {
            "mean": calculate_stats(meanval),
            "max": calculate_stats(maxval)
        }

        if len(meanvalpercore) > 0:
            results['meanpercore'] = calculate_stats(meanvalpercore)
            results['maxpercore'] = calculate_stats(maxvalpercore)

        return results

