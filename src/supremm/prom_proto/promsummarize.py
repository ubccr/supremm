import os
import json
import time
import logging
import requests
import sys
import urllib.parse as urlparse
from collections import OrderedDict

import numpy as np
import prometheus_api_client as pac
from prometheus_api_client.utils import parse_datetime

from supremm.proc_common import filter_plugins, instantiatePlugins
from supremm.plugin import loadpreprocessors, loadplugins, NodeMetadata
from supremm.rangechange import RangeChange


def load_translation():
    """
    Update mapping of available Prometheus metrics
    with corresponding PCP metric names.
    """        
    # Load mapping
    prom2pcp = {}
    version = "v3"
    file = "mapping/%s.json" % (version)
    with open(file, "r") as f:
        prom2pcp = json.load(f)

    logging.debug("Available metric mapping(s): \n{}".format(json.dumps(prom2pcp, indent=2)))
    return prom2pcp        

class NodeMeta(NodeMetadata):
    """ container for node metadata """
    def __init__(self, nodename):
        self._nodename = nodename
        self._nodeidx = 0
        self._archivedata = None

    nodename = property(lambda self: self._nodename)
    nodeindex = property(lambda self: self._nodeidx)
    archive = property(lambda self: self._archivedata)

class PromSummarize():
    def __init__(self, preprocessors, analytics, job):
        # Establish connection with server:
        self.url = "http://172.22.0.216:9090"
        self.connect = pac.PrometheusConnect(url=self.url, disable_ssl=True)

        # Translation Prom -> PCP metric names
        #self.available_metrics = self.connect.all_metrics()
        self.valid_metrics = load_translation()

        # Standard summarization attributes
        self.preprocs = preprocessors
        self.firstlast = [x for x in analytics if x.mode == "firstlast"]
        self.alltimestamps = [x for x in analytics if x.mode in ("all", "timeseries")]
        
        self.job = job

    def get(self):
        logging.info("Returning summary information")
        output = {}
        timeseries = {}

        for analytic in self.alltimestamps:
            if analytic.status != "uninitialized":
                if analytic.mode == "all":
                    output[analytic.name] = analytic.results()
                if analytic.mode == "timeseries":
                    timeseries[analytic.name] = analytic.results()
        for analytic in self.firstlast:
            if analytic.status != "uninitialized": 
                output[analytic.name] = analytic.results()

        if len(timeseries) > 0:
            output['timeseries'] = timeseries

        for preproc in self.preprocs:
            result = preproc.results()
            if result != None:
                output.update(result)

        return output

    def process(self):
        # For now just a single node
        nodelist = ["prometheus-dev.ccr.xdmod.org"]
        for nodename in nodelist:
            logging.info("Processing node %s", nodename)
            node_proc_start = time.time()

            self.process_node(nodename)

            node_proc_time = time.time() - node_proc_start
            logging.debug("%s summarized in %s seconds" % (nodename, node_proc_time))

    def process_node(self, nodename):
        # Create metadata from nodename
        mdata = NodeMeta(nodename)

        for preproc in self.preprocs:
            reqMetrics = self.metric_mapping(preproc.requiredMetrics)
            if False == reqMetrics:
                logging.warning("Skipping %s (%s). No metric mapping available." % (type(preproc).__name__, preproc.name))
                continue
            logging.debug("Processing %s for %s.", preproc.name, nodename)
            self.processforpreproc(mdata, preproc, reqMetrics)

        for analytic in self.alltimestamps:
            reqMetrics = self.metric_mapping(analytic.requiredMetrics)
            if False == reqMetrics:
                logging.warning("Skipping %s (%s). No metric mapping available." % (type(analytic).__name__, analytic.name))
                continue
            logging.debug("Processing %s for %s.", analytic.name, nodename)
            self.processforanalytic(nodename, analytic, mdata, reqMetrics)

        for analytic in self.firstlast:
            reqMetrics = self.metric_mapping(analytic.requiredMetrics)
            if False == reqMetrics:
                logging.warning("Skipping %s (%s). No metric mapping available." % (type(analytic).__name__, analytic.name))
                continue
            logging.debug("Processing %s for %s", analytic.name, nodename)
            self.processfirstlast(nodename, analytic, mdata, reqMetrics)

    def processforpreproc(self, mdata, preproc, reqMetrics):
        start, end = self.job.start_datetime, self.job.end_datetime

        #available = self.timeseries_meta(start, end, reqMetrics.values())
        ## Currently only checks if there is no data, assumes that if there is data then all timeseries are present
        #if not available:
        #    logging.warning("Skipping %s (%s). No data available." % (type(preproc).__name__, preproc.name))
        #    preproc.status = "failure"
        #    preproc.hostend()
        #    return
        preproc.hoststart(mdata.nodename)
        
        start = parse_datetime(start)
        end = parse_datetime(end)
        timestep = "1h"

        rdata = [self.connect.custom_query_range(metric, start, end, timestep) for metric in reqMetrics.values()]
        for ts, d in formatforplugin(rdata, "matrix"):
            if False == self.runpreproccall(preproc, mdata, d, ts):
                break
        
        preproc.status = "complete"
        preproc.hostend()

    def processfirstlast(self, nodename, analytic, mdata, reqMetrics):
        # Query if timeseries exists at given timestamp
        start, end = self.job.start_datetime, self.job.end_datetime

        #available = self.timeseries_meta(start, end, reqMetrics.values())
        ## Currently only checks if there is no data, assumes that if there is data then all timeseries are present
        #if not available:
        #    logging.warning("Skipping %s (%s). No data available." % (type(analytic).__name__, analytic.name))
        #    analytic.status = "failure"
        #    return

        for t in (start, end):
            rdata = [self.connect.custom_query(query=m, params={'time':t}) for m in reqMetrics.values()]
            ts, pdata = formatforplugin(rdata, "vector")
            self.runcallback(analytic, mdata, pdata, ts)

        analytic.status = "complete"

    def processforanalytic(self, nodename, analytic, mdata, reqMetrics):
        start, end = self.job.start_datetime, self.job.end_datetime

        #available = self.timeseries_meta(start, end, reqMetrics.values())
        # Currently only checks if there is no data, assumes that if there is data then all timeseries are present
        #if not available:
        #    logging.warning("Skipping %s (%s). No data available." % (type(analytic).__name__, analytic.name))
        #    analytic.status = "failure"
        #    return

        start = parse_datetime(start)
        end = parse_datetime(end)
        timestep = "1h"
        
        rdata = [self.connect.custom_query_range(metric, start, end, timestep) for metric in reqMetrics.values()]
        for ts, d in formatforplugin(rdata, "matrix"):
            self.runcallback(analytic, mdata, d, ts)
        
        analytic.status = "complete"

    def runpreproccall(self, preproc, mdata, pdata, ts):
        """ Call the pre-processor data processing function 
            Comment from pcp_common/pcpcinterface/pcpcinterface.pyx
            function: extractValues
            data is in format: list (entry for each pmid)
                        |--> list (entry for each instance)
                                |--> list (pmid 0, instance 0)
                                        |--> value
                                        |--> instance
                                |--> list (pmid0, instance 1)
                                        ...
                                ...
                        ...
    
        """
        # Format for preproc like above
        data = []
        if len(pdata[0]) == 1:
            data.append([[float(pdata[0][0]),-1]])
        else:
            for m in pdata:
                datum = []
                for idx, v in enumerate(m):
                    datum.append((float(v), idx))
                data = [datum]

        preproc_data = np.array(data)
        retval = preproc.process(timestamp=ts, data=preproc_data, description=[["",""],["", ""]])
        return retval

    def runcallback(self, analytic, mdata, pdata, ts):
        """ Call the plugin data processing function """
        callback_start = time.time()
        plugin_data = [np.array(datum, dtype=np.float) for datum in pdata]
        retval = analytic.process(nodemeta=mdata, timestamp=ts, data=plugin_data, description=[["",""],["", ""]])

        callback_time = time.time() - callback_start
        return retval

    def metric_mapping(self, reqMetrics):
        """
        Recursively checks if a mapping is available from a given metrics list or list of lists

        params: reqMetrics - list of metrics from preproc/plugin 
        return: OrderedDict of the PCP to Prometheus mapping
                False if a mapping is not present.
        """        
        if isinstance(reqMetrics[0], list):
            for metriclist in reqMetrics:
                mapping = self.metric_mapping(metriclist)
                if mapping:
                    return mapping
            return False
        else:
            mapping = OrderedDict.fromkeys(reqMetrics, None)
            for k, v in mapping.items():
                try:
                    mapping[k] = self.valid_metrics[k]
                except KeyError:
                    logging.warning("Mapping unavailable for metric: %s", k)
                    return False
            return mapping

    def timeseries_meta(self, start, end, matches):
        # This is basis for checking if timeseries is available
        # General form matches.append("{__name__=%s, instance=%s}" %s (metric_name, nodename))
        # http://172.22.0.216:9090/api/v1/series?start=&end=' --data-urlencode 'match[]=prom_metric_name{label="labelname"}'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        params = {
            'match[]': matches,
            'start': str(start),
            'end': str(end)
        }

        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self.url, "/api/v1/series")
        logging.debug('Prometheus QUERY SERIES META, url="%s" start=%s end=%s', url, start, end)
        r = requests.post(url, data=params, headers=headers)
        if r.status_code != 200:
            return False

        data = r.json()
        # data is a list of valid queries to pass along elsewhere
        return data["data"]

def formatforplugin(rdata, rtype):
    if rtype == "vector":
        return formatvector(rdata)

    # Process matrix
    elif rtype == "matrix":
        return formatmatrix(rdata)

def formatvector(rdata):
    pdata = []
    ts = rdata[0][0]["value"][0]
    for m in rdata:
        pdata.append([m[0]["value"][1]])
    return ts, pdata

def formatmatrix(rdata):
    for idx, val in enumerate(rdata[0][0]['values']):
        ts = val[0]
        pdata = []
        for r in rdata:
            pdata.append([m['values'][idx][1] for m in r])
        yield ts, pdata

