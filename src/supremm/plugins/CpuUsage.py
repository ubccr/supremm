#!/usr/bin/env python3
""" CPU Usage metrics """

from supremm.plugin import Plugin
from supremm.statistics import calculate_stats
from supremm.errors import ProcessingError
import numpy

class CpuUsage(Plugin):
    """ Compute the overall cpu usage for a job """

    name = property(lambda x: "cpu")
    mode = property(lambda x: "firstlast")
    requiredMetrics = property(lambda x: [[
            "kernel.percpu.cpu.user", 
            "kernel.percpu.cpu.idle", 
            "kernel.percpu.cpu.nice",
            "kernel.percpu.cpu.sys", 
            "kernel.percpu.cpu.wait.total",
            "kernel.percpu.cpu.irq.hard",
            "kernel.percpu.cpu.irq.soft"
        ], [
            "kernel.percpu.cpu.user", 
            "kernel.percpu.cpu.idle", 
            "kernel.percpu.cpu.sys", 
            "kernel.percpu.cpu.wait.total"
        ], [
            "kernel.all.cpu.user",
            "kernel.all.cpu.idle",
            "kernel.all.cpu.nice",
            "kernel.all.cpu.sys",
            "kernel.all.cpu.wait.total"
        ]])

    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(CpuUsage, self).__init__(job)
        self._first = {}
        self._last = {}
        self._totalcores = 0
        self._outnames = None
        self._ncpumetrics = -1

    def process(self, nodemeta, timestamp, data, description):

        if self._ncpumetrics == -1:
            self._ncpumetrics = len(data)
        elif len(data) != self._ncpumetrics:
            return False

        if data[0].size == 0:
            return False

        if nodemeta.nodename not in self._first:
            self._first[nodemeta.nodename] = numpy.array(data)
            return True

        self._last[nodemeta.nodename] = numpy.array(data)
        self._totalcores += data[0].size
        return True

    def computeallcpus(self):
        """ overall stats for all cores on the nodes """

        ratios = numpy.empty((self._ncpumetrics, self._totalcores), numpy.double)

        coreindex = 0
        for host, last in self._last.items():
            try:
                elapsed = last - self._first[host]

                if numpy.amin(numpy.sum(elapsed, 0)) < 1.0:
                    # typically happens if the job was very short and the datapoints are too close together
                    return {"error": ProcessingError.JOB_TOO_SHORT}

                coresperhost = len(last[0, :])
                ratios[:, coreindex:(coreindex+coresperhost)] = 1.0 * elapsed / numpy.sum(elapsed, 0)
                coreindex += coresperhost
            except ValueError:
                # typically happens if the linux pmda crashes during the job
                return {"error": ProcessingError.INSUFFICIENT_DATA}
 
        results = {}
        for i, name in enumerate(self._outnames):
            results[name] = calculate_stats(ratios[i, :])
 
        results['all'] = {"cnt": self._totalcores}
 
        return results


    def computejobcpus(self):
        """ stats for the cores on the nodes that were assigend to the job (if available) """

        proc = self._job.getdata(preproc)

        if proc == None:
            return {"error": ProcessingError.CPUSET_UNKNOWN}, {"error": ProcessingError.CPUSET_UNKNOWN}

        cpusallowed = proc['cpusallowed']

        ratios = numpy.empty((self._ncpumetrics, self._totalcores), numpy.double)

        coreindex = 0
        for host, last in self._last.items():
            elapsed = last - self._first[host]
            if host in cpusallowed and 'error' not in cpusallowed[host]:
                elapsed = elapsed[:, cpusallowed[host]]
            else:
                return {"error": ProcessingError.CPUSET_UNKNOWN}, {"error": ProcessingError.CPUSET_UNKNOWN}

            coresperhost = len(elapsed[0, :])
            ratios[:, coreindex:(coreindex+coresperhost)] = 1.0 * elapsed / numpy.sum(elapsed, 0)
            coreindex += coresperhost

        allowedcores = numpy.array(ratios[:, :coreindex])

        results = {}
        for i, name in enumerate(self._outnames):
            results[name] = calculate_stats(allowedcores[i, :])

        results['all'] = {"cnt": coreindex}

        effective = numpy.compress(allowedcores[1, :] < 0.95, allowedcores , axis=1)
        effectiveresults = {
            'all': len(effective[i, :])
        }
        if effectiveresults['all'] > 0:
            for i, name in enumerate(self._outnames):
                effectiveresults[name] = calculate_stats(effective[i, :])

        return results, effectiveresults
        

    def results(self):

        nhosts = len(self._last)

        if nhosts < 1:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        if self._ncpumetrics == 7:
            self._outnames = ["user", "idle", "nice", "system", "iowait", "irq", "softirq"] 
        elif self._ncpumetrics == 4:
            self._outnames = ["user", "idle", "system", "iowait"]
        else:
            self._outnames = ["user", "idle", "nice", "system", "iowait"]

        nodecpus = self.computeallcpus()
        if "error" not in nodecpus:
            jobcpus, effcpus = self.computejobcpus()
        else:
            jobcpus = nodecpus
            effcpus = nodecpus
            

        return {"nodecpus": nodecpus, "jobcpus": jobcpus, "effcpus": effcpus}
