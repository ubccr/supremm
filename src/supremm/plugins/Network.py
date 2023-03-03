#!/usr/bin/env python3

from supremm.plugin import DeviceBasedPlugin

class Network(DeviceBasedPlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "network")
    requiredMetrics = property(lambda x: [
        "network.interface.in.bytes",
        "network.interface.out.bytes",
        ])
    optionalMetrics = property(lambda x: [
        "network.interface.in.packets"
        "network.interface.out.packets"
        ])
    derivedMetrics = property(lambda x: [])


