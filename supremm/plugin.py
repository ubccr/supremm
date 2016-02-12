""" definition of the plugin API and implementations of some base classes that
    include common functions """

from abc import ABCMeta, abstractmethod, abstractproperty
from supremm.statistics import calculate_stats
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError
import os
import numpy
import pkgutil

from sys import version as python_version
if python_version.startswith("2.6"):
    from backport_collections import Counter
else:
    from collections import Counter


def loadplugins(plugindir=None, namespace="plugins"):
    """ Load all of the modules from the plugins directory and instantiate the
        classes within. """

    if plugindir == None:
        plugindir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plugins")

    analytics = []

    for _, x, _ in pkgutil.iter_modules([plugindir]):
        m1 = __import__('supremm.' + namespace + '.' + x)
        m = getattr(m1, namespace)
        m = getattr(m, x)
        c = getattr(m, x)
        if issubclass(c, Plugin) or issubclass(c, PreProcessor):
            analytics.append(c)
        else:
            del m1

    return analytics


def loadpreprocessors(preprocdir=None):
    """ preprocessors have same api as plugins. The difference is that they
        are called first so the results are available to the plugins.
        There is no dependency management as of yet so a preproc cannot require
        another preproc.
    """
    if preprocdir == None:
        preprocdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "preprocessors")

    return loadplugins(preprocdir, "preprocessors")

class NodeMetadata(object):
    """ Wrapper class that contains info about a job node. This is passed to
        the process function of the plugin. """
    __metaclass__ = ABCMeta

    @property
    @abstractmethod
    def nodename(self):
        """ Returns a unique string identifier for the node (typically the hostname) """
        pass

    @property
    @abstractmethod
    def nodeindex(self):
        """ returns a unique numerical identifier for the node """
        pass

class Plugin(object):
    """ abstract base class describing the plugin interface """
    __metaclass__ = ABCMeta

    def __init__(self, job):
        self._job = job
        self._status = "uninitialized"

    @property
    def status(self):
        """ The status state is used by the framework to decide whether to include the
            plugin data in the output """
        return self._status

    @status.setter
    def status(self, value):
        """ status can be set by the framework """
        self._status = value

    @abstractmethod
    def process(self, nodemeta, timestamp, data, description):
        """ process is called for every requested data point """
        pass

    @abstractmethod
    def results(self):
        """ results will be called once after all the datapoints have had calls to  process()"""
        pass

    @abstractproperty
    def name(self):
        pass

    @abstractproperty
    def requiredMetrics(self):
        pass

    @abstractproperty
    def optionalMetrics(self):
        pass

    @abstractproperty
    def derivedMetrics(self):
        pass

    @abstractproperty
    def mode(self):
        pass


class PreProcessor(object):
    """
    Preprocessors are called on each archive before all of the plugins are
    run. The results of the preprocessor are therefore available to the plugins.
    The preprocessor results should be added to the job object by the preprocessor
    using the job.addata() function.
    """
    __metaclass__ = ABCMeta

    def __init__(self, job):
        self._job = job
        self._status = "uninitialized"

    @property
    def status(self):
        """ The status state is used by the framework to decide whether to include the
            plugin data in the output """
        return self._status

    @status.setter
    def status(self, value):
        """ status can be set by the framework """
        self._status = value

    @abstractmethod
    def hoststart(self, hostname):
        """ Called by the framework for all hosts assigned to the job whether or not they contain
            the required metrics.
        """
        pass

    @abstractmethod
    def process(self, timestamp, data, description):
        """ Called by the framework for all datapoints collected for the host
            referenced in the preceeding call to hoststart. It a host has no data
            then this will not be called.
        """
        pass

    @abstractmethod
    def results(self):
        """ preprocessors may return a results object that will be added to the summary
        document.
        """
        pass

    @abstractmethod
    def hostend(self):
        """ Called after all of the data available for a host has been processed. """
        pass

    @abstractproperty
    def name(self):
        pass

    @abstractproperty
    def requiredMetrics(self):
        pass

    @abstractproperty
    def optionalMetrics(self):
        pass

    @abstractproperty
    def derivedMetrics(self):
        pass

    @abstractproperty
    def mode(self):
        pass


class DeviceBasedPlugin(Plugin):
    """ 
    A base abstract class for summarising the job-delta for device-based metrics
    The plugin name and list of required metrics must be provided by the implementation
    """
    __metaclass__ = ABCMeta

    mode = property(lambda x: "firstlast")

    def __init__(self, job):
        super(DeviceBasedPlugin, self).__init__(job)
        self._first = {}
        self._data = {}


    def process(self, nodemeta, timestamp, data, description):

        if len(data[0]) == 0:
            return False

        if nodemeta.nodename not in self._first:
            self._first[nodemeta.nodename] = data
            return True

        hostdata = numpy.array(data) - self._first[nodemeta.nodename]

        for mindex, i in enumerate(description):
            for index in xrange(len(hostdata[mindex, :])):
                indom = i[1][index]
                metricname = self.requiredMetrics[mindex]

                if indom not in self._data:
                    self._data[indom] = {}
                if metricname not in self._data[indom]:
                    self._data[indom][metricname] = []
                self._data[indom][metricname].append(hostdata[mindex, index])

        return True

    def results(self):

        if len(self._data) == 0:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        output = {}

        for devicename, device in self._data.iteritems():
            cleandevname = devicename.replace(".", "-")
            output[cleandevname] = {}
            for metricname, metric in device.iteritems():
                prettyname = "-".join(metricname.split(".")[2:])
                output[cleandevname][prettyname] = calculate_stats(metric)

        return output


class RateConvertingTimeseriesPlugin(Plugin):
    """ 
    A base abstract class for generating a timeseries summary for values that should
    be converted to rates, one per node.
    The plugin name,  list of required metrics and generator function must be provided by the implementation
    """
    __metaclass__ = ABCMeta

    mode = property(lambda x: "timeseries")

    def __init__(self, job):
        super(RateConvertingTimeseriesPlugin, self).__init__(job)
        self._data = TimeseriesAccumulator(job.nodecount, self._job.walltime)
        self._hostdata = {}

    @abstractmethod
    def computetimepoint(self, data):
        """ Called with the data for each timepoint on each host """
        pass

    def process(self, nodemeta, timestamp, data, description):

        if nodemeta.nodeindex not in self._hostdata:
            self._hostdata[nodemeta.nodeindex] = 1

        self._data.adddata(nodemeta.nodeindex, timestamp, self.computetimepoint(data))

    def results(self):

        if len(self._hostdata) != self._job.nodecount:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        values = self._data.get()

        if len(values[0, :, 0]) < 3:
            return {"error": ProcessingError.JOB_TOO_SHORT}

        rates = numpy.diff(values[:, :, 1]) / numpy.diff(values[:, :, 0])

        if len(self._hostdata) > 64:

            # Compute min, max & median data and only save the host data
            # for these hosts

            sortarr = numpy.argsort(rates.T, axis=1)

            retdata = {
                "min": self.collatedata(sortarr[:, 0], rates),
                "max": self.collatedata(sortarr[:, -1], rates),
                "med": self.collatedata(sortarr[:, sortarr.shape[1] / 2], rates),
                "times": values[0, 1:, 0].tolist(),
                "hosts": {}
            }

            uniqhosts = Counter(sortarr[:, 0])
            uniqhosts.update(sortarr[:, -1])
            uniqhosts.update(sortarr[:, sortarr.shape[1] / 2])
            includelist = uniqhosts.keys()
        else:
            # Save data for all hosts
            retdata = {
                "times": values[0, 1:, 0].tolist(),
                "hosts": {}
            }
            includelist = self._hostdata.keys()


        for hostidx in includelist:
            retdata['hosts'][str(hostidx)] = {}
            retdata['hosts'][str(hostidx)]['all'] = rates[hostidx, :].tolist()

        return retdata

    @staticmethod
    def collatedata(args, rates):
        """ build output data """
        result = []
        for timepoint, hostidx in enumerate(args):
            try:
                result.append([rates[hostidx, timepoint], int(hostidx)])
            except IndexError:
                pass

        return result
