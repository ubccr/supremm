#!/usr/bin/env python
""" hardware inventory pre-processor """

from supremm.plugin import PreProcessor

class HardwareInventory(PreProcessor):
    """ Parse and analyse hardware inventory information. Currently
        grabs the number of CPU cores for each host.
    """

    name = property(lambda x: "hinv")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: ["kernel.percpu.cpu.user"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(HardwareInventory, self).__init__(job)
        self.hostname = None
        self.corecount = None
        self.data = {}

    def hoststart(self, hostname):
        self.hostname = hostname

    def process(self, timestamp, data, description):

        if len(data) == 1 and data[0][:, 0].size > 0:
            self.corecount = data[0][:, 0].size
            # Have sufficient information, therefore return False to prevent
            # any further callbacks
            return False

        return True

    def hostend(self):
        if self.corecount != None:
            self.data[self.hostname] = {'cores': self.corecount}

        self.corecount = None
        self.hostname = None

        self._job.adddata(self.name, self.data)

    def results(self):
        return None

