#!/usr/bin/env python3
""" Plugin for debugging firstlast plugin behavior in Prometheus summarization code """

from supremm.plugin import Plugin

class Debug(Plugin):
    """ Various test cases regarding firstlast plugin callbacks can be used here """

    name = property(lambda x: "debug")
    mode = property(lambda x: "firstlast")
    requiredMetrics = property(lambda x: ["DEBUG"])

    optionalMetrics = property(lambda x: [])

    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(Debug, self).__init__(job)
        self._numcallbacks = 0
        
    def process(self, nodemeta=None, timestamp=None, data=None, description=None):
        self._numcallbacks += 1
        
    def results(self):
        return {"callbacks": self._numcallbacks}
