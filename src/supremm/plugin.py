""" definition of the plugin API and implementations of some base classes that
    include common functions """

from abc import ABCMeta, abstractmethod, abstractproperty
from supremm.statistics import calculate_stats
from supremm.subsample import TimeseriesAccumulator
from supremm.errors import ProcessingError
import os
import numpy
import pkgutil
import requests
try:
    import urlparse as urlparse
except LoadError:
    import urllib.parse as urlparse
from collections import Counter
import logging

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

    def __init__(self, job, config):
        self._job = job
        self._status = "uninitialized"
        self._config = config

    @property
    def status(self):
        """ The status state is used by the framework to decide whether to include the
            plugin data in the output """
        return self._status

    @property
    def config(self):
        return self._config

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
    def metric_system(self):
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

    def __init__(self, job, config):
        self._job = job
        self._status = "uninitialized"
        self._config = config

    @property
    def status(self):
        """ The status state is used by the framework to decide whether to include the
            plugin data in the output """
        return self._status

    @property
    def config(self):
        return self._config

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
    def metric_system(self):
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

    def __init__(self, job, config):
        super(DeviceBasedPlugin, self).__init__(job, config)
        self._first = {}
        self._data = {}
        self._error = None
        self.allmetrics = self.requiredMetrics + self.optionalMetrics

    def process(self, nodemeta, timestamp, data, description):

        if len(data[0]) == 0:
            return False

        if nodemeta.nodename not in self._first:
            self._first[nodemeta.nodename] = numpy.array(data)
            return True

        ndata = numpy.array(data)

        if ndata.shape != self._first[nodemeta.nodename].shape:
            self._error = ProcessingError.INDOMS_CHANGED_DURING_JOB
            return False

        hostdata = ndata - self._first[nodemeta.nodename]

        for mindex, i in enumerate(description):
            for index in xrange(len(hostdata[mindex, :])):
                indom = i[1][index]
                metricname = self.allmetrics[mindex]

                if indom not in self._data:
                    self._data[indom] = {}
                if metricname not in self._data[indom]:
                    self._data[indom][metricname] = []
                self._data[indom][metricname].append(hostdata[mindex, index])

        return True

    def results(self):

        if self._error != None:
            return {"error": self._error}

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

class DeviceInstanceBasedPlugin(Plugin):
    """
    A base abstract class for summarising the job-delta for device-based metrics
    that only have a singe instance.
    The plugin name and list of required metrics must be provided by the implementation
    """
    __metaclass__ = ABCMeta

    mode = property(lambda x: "firstlast")

    def __init__(self, job, config):
        super(DeviceInstanceBasedPlugin, self).__init__(job, config)
        self._first = {}
        self._data = {}
        self._error = None

    def process(self, nodemeta, timestamp, data, description):

        if len(data[0]) == 0:
            return False

        if nodemeta.nodeindex not in self._first:
            self._first[nodemeta.nodeindex] = numpy.array(data)
            return True

        hostdata = numpy.array(data) - self._first[nodemeta.nodeindex]

        for idx, metricname in enumerate(self.requiredMetrics):
            if metricname not in self._data:
                self._data[metricname] = []
            self._data[metricname].append(hostdata[idx, 0])

    def results(self):

        if self._error != None:
            return {"error": self._error}

        if len(self._data) == 0:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        output = {}

        for metricname, metric in self._data.iteritems():
            prettyname = "-".join(metricname.split(".")[1:])
            output[prettyname] = calculate_stats(metric)

        return output


class RateConvertingTimeseriesPlugin(Plugin):
    """ 
    A base abstract class for generating a timeseries summary for values that should
    be converted to rates, one per node.
    The plugin name,  list of required metrics and generator function must be provided by the implementation
    """
    __metaclass__ = ABCMeta

    mode = property(lambda x: "timeseries")

    def __init__(self, job, config):
        super(RateConvertingTimeseriesPlugin, self).__init__(job, config)
        self._data = TimeseriesAccumulator(job.nodecount, self._job.walltime)
        self._hostdata = {}

    @abstractmethod
    def computetimepoint(self, data):
        """ Called with the data for each timepoint on each host """
        pass

    def process(self, nodemeta, timestamp, data, description):

        if nodemeta.nodeindex not in self._hostdata:
            self._hostdata[nodemeta.nodeindex] = 1

        datum = self.computetimepoint(data)
        if datum != None:
            self._data.adddata(nodemeta.nodeindex, timestamp, datum)

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

class PrometheusPlugin(Plugin):
    """
    A base abstract class for summarising the data from Prometheus
    """
    __metaclass__ = ABCMeta

    mode = property(lambda x: "all")

    def __init__(self, job, config):
        super(PrometheusPlugin, self).__init__(job, config)
        self._first = {}
        self._data = {}
        self._error = None
        self.allmetrics = {}
        for k,v in self.requiredMetrics.items():
            self.allmetrics[k] = v
        for k,v in self.optionalMetrics.items():
            self.allmetrics[k] = v

        self.metric_configs = config.metric_configs()
        self.prometheus_url = self.metric_configs.get('prometheus_url', None)
        self.step = self.metric_configs.get('step', '1m')

    def query(self, query, start, end):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        params = {
            'query': query,
            'start': str(start),
            'end': str(end),
            'step': self.step,
        }
        url = urlparse.urljoin(self.prometheus_url, "/api/v1/query_range")
        logging.debug('Prometheus QUERY, url="%s" params="%s"', url, params)
        r = requests.post(url, data=params, headers=headers)
        if r.status_code != 200:
            return None
        data = r.json()
        return data

    def process(self, mdata):
        rate = self.metric_configs.get('rates', {}).get(self.name, '5m')
        for metricname, metric in self.allmetrics.items():
            indom_label = metric.get('indom', None)
            query = metric['metric'].format(node=mdata.nodename, rate=rate)
            data = self.query(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                if indom_label:
                    indom = r.get('metric', {}).get(indom_label, None)
                else:
                    indom = 'NA'
                if indom is None:
                    logging.error("Unable to find Prometheus label to match requested indom=%s", metric.indom)
                    continue
                if indom_label and indom not in self._data:
                    self._data[indom] = {}
                if not indom_label and metricname not in self._data:
                    self._data[metricname] = {}
                if indom_label and metricname not in self._data[indom]:
                    self._data[indom][metricname] = []
                for v in r.get('values', []):
                    value = float(v[1])
                    if indom == 'NA':
                        self._data[metricname].append(value)
                    else:
                        self._data[indom][metricname].append(value)
        return True

    def results(self):

        if self._error != None:
            return {"error": self._error}

        if len(self._data) == 0:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        output = {}

        for devicename, device in self._data.iteritems():
            cleandevname = devicename.replace(".", "-")
            output[cleandevname] = {}
            for metricname, metric in device.iteritems():
                output[cleandevname][metricname] = calculate_stats(metric)

        return output

class PrometheusTimeseriesPlugin(PrometheusPlugin):
    """
    A base abstract class for summarising the data from Prometheus
    """
    __metaclass__ = ABCMeta

    mode = property(lambda x: "timeseries")

    def __init__(self, job, config):
        super(PrometheusTimeseriesPlugin, self).__init__(job, config)
        self._data = TimeseriesAccumulator(job.nodecount, self._job.walltime)
        self._hostdata = {}

    def process(self, mdata):
        for metricname, metric in self.allmetrics.items():
            query = metric['metric'].format(node=mdata.nodename, jobid=self._job.job_id, rate='5m')
            data = self.query(query, mdata.start, mdata.end)
            if data is None:
                self._error = ProcessingError.PROMETHEUS_QUERY_ERROR
                return None
            for r in data.get('data', {}).get('result', []):
                if mdata.nodeindex not in self._hostdata:
                    self._hostdata[mdata.nodeindex] = 1
                for v in r.get('values', []):
                    value = float(v[1])
                    self._data.adddata(mdata.nodeindex, v[0], value)
        return True

    def results(self):
        if self._error != None:
            return {"error": self._error}
        if len(self._hostdata) != self._job.nodecount:
            return {"error": ProcessingError.INSUFFICIENT_HOSTDATA}

        values = self._data.get()

        if len(values[0, :, 0]) < 3:
            return {"error": ProcessingError.JOB_TOO_SHORT}

        data = values[:, :, 1]

        if len(self._hostdata) > 64:

            # Compute min, max & median data and only save the host data
            # for these hosts

            sortarr = numpy.argsort(data.T, axis=1)

            retdata = {
                "min": self.collatedata(sortarr[:, 0], data),
                "max": self.collatedata(sortarr[:, -1], data),
                "med": self.collatedata(sortarr[:, sortarr.shape[1] / 2], data),
                "times": values[:, :, 0].tolist(),
                "hosts": {}
            }

            uniqhosts = Counter(sortarr[:, 0])
            uniqhosts.update(sortarr[:, -1])
            uniqhosts.update(sortarr[:, sortarr.shape[1] / 2])
            includelist = uniqhosts.keys()
        else:
            # Save data for all hosts
            retdata = {
                "times": values[:, :, 0].tolist(),
                "hosts": {}
            }
            includelist = self._hostdata.keys()


        for hostidx in includelist:
            retdata['hosts'][str(hostidx)] = {}
            retdata['hosts'][str(hostidx)]['all'] = values[hostidx, :, 1].tolist()

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
