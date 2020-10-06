#!/usr/bin/env python
""" Energy usage plugin - https://github.com/NVIDIA/gpu-monitoring-tools"""

from supremm.plugin import PrometheusPlugin
from supremm.statistics import RollingStats, calculate_stats, Integrator
from supremm.errors import ProcessingError

class GpuPowerPrometheus(PrometheusPlugin):
    """ Compute the power statistics for a job """

    name = property(lambda x: "gpupower")
    metric_system = property(lambda x: "prometheus")
    requiredMetrics = property(lambda x: {
        'power': {
            'metric': 'DCGM_FI_DEV_POWER_USAGE{{instance=~"^{node}.+"}}',
        }
    })
    optionalMetrics = property(lambda x: {})
    derivedMetrics = property(lambda x: {})

    def process(self, mdata):
        """ Power measurements are similar to the memory measurements the first and last data points
        are ignored and the statistics are computed over all of the other measurements.
        """
        self._data[mdata.nodename] = {}
        for metricname, metric in self.allmetrics.items():
            query = metric['metric'].format(node=mdata.nodename, rate=self.rate)
            data = self.query_range(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                gpu = r.get('metric', {}).get('gpu', None)
                if gpu is None:
                    self._error = ProcessingError.INSUFFICIENT_DATA
                    return False
                name = "gpu%s" % gpu
                if name not in self._data[mdata.nodename]:
                    self._data[mdata.nodename][name] = {
                        'power': RollingStats(),
                        'energy': Integrator(mdata.start),
                    }
                for v in r.get('values', []):
                    ts = v[0]
                    value = float(v[1])
                    self._data[mdata.nodename][name]['power'].append(value)
                    self._data[mdata.nodename][name]['energy'].add(ts, value)

        return True

    def results(self):
        result = {}
        for nodename, devices in self._data.items():
            for devicename, data in devices.items():
                if data['power'].count() < 1:
                    continue
                if devicename not in result:
                    result[devicename] = {"meanpower": [], "maxpower": [], "energy": []}
                result[devicename]["meanpower"].append(data['power'].mean())
                result[devicename]["maxpower"].append(data['power'].max)
                result[devicename]["energy"].append(data['energy'].total)

        if not result:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        output = {}
        for device, data in result.items():
            output[device] = {
                "power": {
                    "mean": calculate_stats(data['meanpower']),
                    "max": calculate_stats(data['maxpower'])
                },
                "energy": calculate_stats(data['energy'])
            }
            output[device]['energy']['total'] = output[device]['energy']['avg'] * output[device]['energy']['cnt']

        return output
