#!/usr/bin/env python

from supremm.plugin import DeviceBasedPlugin

class Gpfs(DeviceBasedPlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "gpfs")
    metric_system = property(lambda x: "pcp")
    requiredMetrics = property(lambda x: [
        "gpfs.fsios.read_bytes",
        "gpfs.fsios.write_bytes",
        "gpfs.fsios.reads",
        "gpfs.fsios.writes"
        ])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])


