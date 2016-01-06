#!/usr/bin/env python
""" GPU statistics """

from supremm.plugin import Plugin
from supremm.statistics import RollingStats, calculate_stats

class GpuUsage(Plugin):
    """ Compute the overall gpu usage for a job """

    name = property(lambda x: "gpu")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["nvidia.gpuactive", "nvidia.memused", "nvidia.memactive"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(GpuUsage, self).__init__(job)
        self._data = {}

    def process(self, nodemeta, timestamp, data, description):

        if len(description) == 0 or len(data[0]) == 0:
            # nvidia pmda can be running, but no cards present
            return False

        if nodemeta.nodename not in self._data:
            self._data[nodemeta.nodename] = {}
            self._data[nodemeta.nodename] = {'gpuactive': RollingStats(), 'memused': RollingStats(), 'memactive': RollingStats()}
            self._data[nodemeta.nodename]['names'] = [x for x in description[0][1]]

        self._data[nodemeta.nodename]['gpuactive'].append(1.0 * data[0])
        self._data[nodemeta.nodename]['memused'].append(1.0 * data[1])
        self._data[nodemeta.nodename]['memactive'].append(1.0 * data[2])

        return True

    def results(self):

        result = {}
        for data in self._data.itervalues():
            for i, devicename in enumerate(data['names']):
                if devicename not in result:
                    result[devicename] = {'gpuactive': [], 'memused': [], 'memactive': []}
                for statname in ['gpuactive', 'memused', 'memactive']:
                    result[devicename][statname].append(data[statname].mean()[i])
            
        output = {}
        for device, data in result.iteritems():
            output[device] = {}
            for statname, datalist in data.iteritems():
                output[device][statname] = calculate_stats(datalist)

        if len(output) == 0:
            output['error'] = "no data"

        return output
