#!/usr/bin/env python

from supremm.plugin import DeviceBasedPlugin

class Block(DeviceBasedPlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "block")
    metric_system = property(lambda x: "pcp")
    requiredMetrics = property(lambda x: [
        "disk.dev.read",
        "disk.dev.read_bytes",
        "disk.dev.write",
        "disk.dev.write_bytes"
        ])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])
