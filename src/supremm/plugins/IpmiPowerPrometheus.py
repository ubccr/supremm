#!/usr/bin/env python
""" Energy usage plugin - https://github.com/soundcloud/ipmi_exporter"""

from supremm.plugin import PrometheusPlugin
from supremm.statistics import RollingStats, calculate_stats, Integrator
from supremm.errors import ProcessingError
import numpy

class IpmiPowerPrometheus(PrometheusPlugin):
    """ Compute the power statistics for a job """

    name = property(lambda x: "ipmi")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'power': {
            'metric': 'ipmi_dcmi_power_consumption_watts{{instance=~"^{node}.+"}}'
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def process(self, mdata):
        self._data[mdata.nodeindex] = {
            'power': RollingStats(),
            'energy': Integrator(mdata.start)
        }
        for metricname, metric in self.allmetrics.items():
            query = metric['metric'].format(node=mdata.nodename, rate=self.rate)
            data = self.query_range(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                for v in r.get('values', []):
                    ts = v[0]
                    value = float(v[1])
                    self._data[mdata.nodeindex]['power'].append(value)
                    self._data[mdata.nodeindex]['energy'].add(ts, value)
        return True

    def results(self):

        meanpower = []
        maxpower = []

        energy = []
        time_covered = 0

        for pdata in self._data.itervalues():
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
