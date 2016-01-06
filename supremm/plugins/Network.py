#!/usr/bin/env python

from supremm.plugin import DeviceBasedPlugin

class Network(DeviceBasedPlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "network")
    requiredMetrics = property(lambda x: [
        "network.interface.in.bytes",
        "network.interface.in.packets",
        "network.interface.out.bytes",
        "network.interface.out.packets"
        ])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])


