#!/usr/bin/env python

from supremm.plugin import DeviceBasedPlugin

class Lustre(DeviceBasedPlugin):
    """ This plugin processes lots of metric that are all interested in the difference over the process """

    name = property(lambda x: "lustre")
    requiredMetrics = property(lambda x: [
        "lustre.llite.read_bytes.total",
        "lustre.llite.write_bytes.total"
        ])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])


