#!/usr/bin/env python3
from supremm.TimeseriesPatterns import TimeseriesPatterns


class TimeseriesPatternsGpfs(TimeseriesPatterns):
    requiredMetrics = property(lambda self: ["gpfs.fsios.read_bytes", "gpfs.fsios.write_bytes"])
    name = property(lambda self: "timeseries_patterns_gpfs")

    def __init__(self, job):
        super(TimeseriesPatternsGpfs, self).__init__(job)
