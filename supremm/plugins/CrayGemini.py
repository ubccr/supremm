#!/usr/bin/env python

from supremm.plugin import DeviceInstanceBasedPlugin

class CrayGemini(DeviceInstanceBasedPlugin):
    """ Metrics from the Cray Gemini interconnect """

    name = property(lambda x: "gemini")
    requiredMetrics = property(lambda x: [
        "gemini.totaloutput_optA",
        "gemini.totalinput",
        "gemini.fmaout",
        "gemini.bteout_optA",
        "gemini.bteout_optB",
        "gemini.totaloutput_optB"
        ])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

