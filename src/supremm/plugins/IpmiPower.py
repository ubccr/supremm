#!/usr/bin/env python
""" Energy usage plugin """

import numpy

from supremm.plugin import Plugin
from supremm.statistics import RollingStats, calculate_stats, Integrator
from supremm.errors import ProcessingError

class IpmiPower(Plugin):
    """ Compute the power statistics for a job """

    name = property(lambda x: "ipmi")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: ["ipmi.dcmi.power"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(IpmiPower, self).__init__(job)
        self._data = {}

    def process(self, nodemeta, timestamp, data, description):
        """ Power measurements are similar to the memory measurements the first and last data points
        are ignored and the statistics are computed over all of the other measurements.
        """

        if not data or data[0].size == 0:
            return True

        if data[0][0] < numpy.finfo(numpy.float64).eps:
            # Some IPMI stacks return a zero value if they don't support power metrics
            return False

        if nodemeta.nodeindex not in self._data:
            self._data[nodemeta.nodeindex] = {
                'power': RollingStats(),
                'energy': Integrator(timestamp)
            }
            return True

        hdata = self._data[nodemeta.nodeindex]

        hdata['power'].append(data[0][0])
        hdata['energy'].add(timestamp, data[0][0])

        return True

    def results(self):

        meanpower = []
        maxpower = []

        energy = []
        time_covered = 0

        for pdata in self._data.values():
            if pdata['power'].count() > 0:
                meanpower.append(pdata['power'].mean())
                maxpower.append(pdata['power'].max)
            energy.append(pdata['energy'].total)
            time_covered += pdata['energy'].elapsed

        total_energy = numpy.sum(energy)

        if total_energy < numpy.finfo(numpy.float64).eps:
            return {"error": ProcessingError.RAW_COUNTER_UNAVAILABLE}

        if time_covered < 0.9 * self._job.nodecount * self._job.walltime:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        if not meanpower:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        energy_stats = calculate_stats(energy)
        energy_stats['total'] = total_energy

        return {
            "power": {
                "mean": calculate_stats(meanpower),
                "max": calculate_stats(maxpower)
            },
            "energy": energy_stats
        }
