#!/usr/bin/env python
""" Energy usage plugin """

from supremm.plugin import Plugin
from supremm.statistics import RollingStats, calculate_stats, Integrator
from supremm.errors import ProcessingError

class GpuPower(Plugin):
    """ Compute the power statistics for a job """

    name = property(lambda x: "gpupower")
    metric_system = property(lambda x: "pcp")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["nvidia.powerused"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job, config):
        super(GpuPower, self).__init__(job, config)
        self._data = {}

    def process(self, nodemeta, timestamp, data, description):
        """ Power measurements are similar to the memory measurements the first and last data points
        are ignored and the statistics are computed over all of the other measurements.
        """

        if not data or not description:
            # nvidia pmda can be running, but no cards present
            return False

        if nodemeta.nodeindex not in self._data:
            self._data[nodemeta.nodeindex] = {
                'power': RollingStats(),
                'energy': Integrator(timestamp),
                'names': [x for x in description[0][1]]
            }
            return True

        hdata = self._data[nodemeta.nodeindex]

        power_watts = data[0] / 1000.0

        hdata['power'].append(power_watts)
        hdata['energy'].add(timestamp, power_watts)

        return True

    def results(self):

        result = {}
        for data in self._data.itervalues():

            if data['power'].count() < 1:
                continue

            for i, devicename in enumerate(data['names']):
                if devicename not in result:
                    result[devicename] = {"meanpower": [], "maxpower": [], "energy": []}

                result[devicename]["meanpower"].append(data['power'].mean()[i])
                result[devicename]["maxpower"].append(data['power'].max[i])
                result[devicename]["energy"].append(data['energy'].total[i])

        if not result:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        output = {}
        for device, data in result.iteritems():
            output[device] = {
                "power": {
                    "mean": calculate_stats(data['meanpower']),
                    "max": calculate_stats(data['maxpower'])
                },
                "energy": calculate_stats(data['energy'])
            }
            output[device]['energy']['total'] = output[device]['energy']['avg'] * output[device]['energy']['cnt']

        return output
