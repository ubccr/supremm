#!/usr/bin/env python3
""" GPU statistics """

from supremm.plugin import Plugin
from supremm.statistics import RollingStats, calculate_stats

class GpuUsage(Plugin):
    """ Compute the overall gpu usage for a job """

    name = property(lambda x: "gpu")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["nvidia.gpuactive", "nvidia.memused"])
    optionalMetrics = property(lambda x: ["nvidia.memactive"])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(GpuUsage, self).__init__(job)
        self._data = {}
        self.statnames = None

    def process(self, nodemeta, timestamp, data, description):

        if len(description) == 0 or len(data[0]) == 0:
            # nvidia pmda can be running, but no cards present
            return False

        if nodemeta.nodename not in self._data:
            if self.statnames == None:
                self.statnames = ['gpuactive', 'memused']
                if len(data) == 3:
                    self.statnames.append('memactive')

            self._data[nodemeta.nodename] = {}
            for statname in self.statnames:
                self._data[nodemeta.nodename][statname] = RollingStats()

            self._data[nodemeta.nodename]['names'] = [x for x in description[0][1]]

        for idx, statname in enumerate(self.statnames):
            self._data[nodemeta.nodename][statname].append(1.0 * data[idx])

        return True

    def results(self):

        result = {}
        for data in self._data.values():
            for i, devicename in enumerate(data['names']):
                if devicename not in result:
                    result[devicename] = {}
                    for statname in self.statnames:
                        result[devicename][statname] = []
                        result[devicename][statname + "max"] = []
                for statname in self.statnames:
                    result[devicename][statname].append(data[statname].mean()[i])
                    result[devicename][statname + "max"].append(data[statname].max[i])
            
        output = {}
        for device, data in result.items():
            output[device] = {}
            for statname, datalist in data.items():
                output[device][statname] = calculate_stats(datalist)

        if len(output) == 0:
            output['error'] = "no data"

        return output
