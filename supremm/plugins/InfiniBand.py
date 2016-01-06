#!/usr/bin/env python

from supremm.plugin import DeviceBasedPlugin

class InfiniBand(DeviceBasedPlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "infiniband")
    requiredMetrics = property(lambda x: [
        "infiniband.port.switch.in.bytes",
        "infiniband.port.switch.in.packets",
        "infiniband.port.switch.out.bytes",
        "infiniband.port.switch.out.packets"
        ])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])
