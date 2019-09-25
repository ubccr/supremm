#!/usr/bin/env python
""" performance counters pre-processor """

from supremm.plugin import PreProcessor

class PerfEvent(PreProcessor):
    """ The hardware performance counters are only valid if they were
        active and counting for the whole job. This preproc checks the active
        flag at all timepoints and the result is avaiable to all the plugins that
        use hardware counters.
    """

    name = property(lambda x: "perf")
    mode = property(lambda x: "timeseries")
    requiredMetrics = property(lambda x: ["perfevent.active"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(PerfEvent, self).__init__(job)
        self.perfactive = None

    def hoststart(self, hostname):
        pass

    def process(self, timestamp, data, description):

        if self.perfactive == False:
            return False

        if len(data) == 1 and data[0].shape == (1, 2) and data[0][:, 0].size > 0:
            self.perfactive = data[0][0, 0] != 0
            return self.perfactive

        return True

    def hostend(self):
        self._job.adddata(self.name, {"active": self.perfactive})

    def results(self):
        return None

