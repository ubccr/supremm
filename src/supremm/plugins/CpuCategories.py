#!/usr/bin/env python
""" CPU categorization plugin """

from supremm.plugin import Plugin
from supremm.errors import ProcessingError
import numpy as np
from collections import OrderedDict

class CpuCategories(Plugin):
    """ Categorize a job based on its CPU utilization """

    name = property(lambda x: "cpucategories")
    mode = property(lambda x: "all")
    requiredMetrics = property(lambda x: [[
        "kernel.percpu.cpu.user",
        "kernel.percpu.cpu.nice",
        "kernel.percpu.cpu.sys",
        "kernel.percpu.cpu.idle",
        "kernel.percpu.cpu.wait.total",
        "kernel.percpu.cpu.intr",
        "kernel.percpu.cpu.irq.soft",
        "kernel.percpu.cpu.irq.hard"
    ]])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    GOOD_THRESHOLD = 0.5
    PINNED_THRESHOLD = 0.9
    LOW_THRESHOLD = 0.1
    DELTA_THRESHOLD = 0.5
    MIN_DELTAS = 5
    MAX_DIFFERENCE = 0.1

    def __init__(self, job):
        super(CpuCategories, self).__init__(job)
        self._timeabove = {}
        self._timebelow = {}
        self._deltas = {}
        self._last = {}
        self._maxcores = {}

    def process(self, nodemeta, timestamp, data, description):
        length = len(data[0])
        node = nodemeta.nodename
        proc = self._job.getdata('proc')

        # Initialize dicts to handle multiple nodes and cores
        if node not in self._last:
            self._timeabove[node] = {}
            self._timebelow[node] = {}
            self._deltas[node] = {}
            self._maxcores[node] = 0

            if proc is None or 'cpusallowed' not in proc or node not in proc['cpusallowed']:
                for i in range(length):
                    self._timeabove[node][i] = 0
                    self._timebelow[node][i] = 0
                    self._deltas[node][i] = []
            else:
                for i in proc['cpusallowed'][node]:
                    self._timeabove[node][i] = 0
                    self._timebelow[node][i] = 0
                    self._deltas[node][i] = []
            self._last[node] = np.array(data)[:, self._timeabove[node].keys()]
            return True

        nodedata = np.array(data)[:, self._timeabove[node].keys()]
        difference = nodedata - self._last[node]
        total = np.sum(difference, 0)
        self._last[node] = nodedata

        currentdeltas = difference[0] / total

        if length != 0:
            counter = 0
            for i in self._timeabove[node]:
                self._deltas[node][i].append(currentdeltas[counter])
                if currentdeltas[counter] > self.DELTA_THRESHOLD:
                    self._timeabove[node][i] += total[counter]
                else:
                    self._timebelow[node][i] += total[counter]
                counter += 1

            totalusage = np.sum(currentdeltas)
            if not np.isnan(totalusage) and int(round(totalusage)) > self._maxcores[node]:
                self._maxcores[node] = int(round(totalusage))
        return True

    def results(self):
        duty_cycles = OrderedDict()
        for node in self._timeabove:
            if len(list(self._deltas[node].itervalues())[0]) < self.MIN_DELTAS:
                return {"error": ProcessingError.INSUFFICIENT_DATA}

            duty_cycles[node] = OrderedDict()
            for i in self._timeabove[node]:
                total_time = self._timeabove[node][i] + self._timebelow[node][i]
                ratio = self._timeabove[node][i] / total_time
                duty_cycles[node]["cpu{}".format(i)] = ratio

        # Categorize the job's performance
        duty_list = np.array([value for node in duty_cycles.itervalues() for value in node.itervalues()])

        if not any(value < self.GOOD_THRESHOLD for value in duty_list):
            category = "GOOD"
        elif not any(value >= self.LOW_THRESHOLD for value in duty_list):
            category = "LOW"
        else:
            high = np.sort(duty_list[duty_list >= self.LOW_THRESHOLD])
            if high.size > 1:
                if high[-1] - high[0] < self.MAX_DIFFERENCE:
                    category = "PINNED"
                else:
                    category = "UNPINNED"
            else:
                if high[0] >= 0.5:
                    category = "PINNED"
                else:
                    category = "UNPINNED"

        return {"dutycycles": duty_cycles, "category": category, "maxcores": sum(self._maxcores.itervalues())}
