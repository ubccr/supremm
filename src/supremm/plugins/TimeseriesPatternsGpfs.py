#!/usr/bin/env python
from supremm.TimeseriesPatterns import TimeseriesPatterns


class TimeseriesPatternsGpfs(TimeseriesPatterns):
    requiredMetrics = property(lambda self: ["gpfs.fsios.read_bytes", "gpfs.fsios.write_bytes"])
    name = property(lambda self: "timeseries_patterns_gpfs")
    metric_system = property(lambda x: "pcp")

    def __init__(self, job, config):
        super(TimeseriesPatternsGpfs, self).__init__(job, config)
