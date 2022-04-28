import os
import json
import time
import logging
import requests
import urllib.parse as urlparse
from collections import OrderedDict
from collections.abc import Iterable

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

    This should be called per-node as different nodes
    could export different metrics.
    """        
    # Load mapping
    prom2pcp = {}
    with open("prom2pcpV2.json") as f:
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
    def __init__(self, analytics, job):
        # Establish connection with server:
        self.url = "http://172.22.0.216:9090"
        self.connect = pac.PrometheusConnect(url=self.url, disable_ssl=True)

        # Translation Prom -> PCP metric names
        #self.available_metrics = self.connect.all_metrics()
        self.valid_metrics = load_translation()

        # Standard summarization attributes
        self.analytics = analytics
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

        return output

    def process(self):
        # Call equivalent of processarchive for every node in job's nodelist
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

        for analytic in self.alltimestamps:
            reqMetrics = self.metric_mapping(analytic.requiredMetrics)
            if False == reqMetrics:
                logging.info("Skipping %s (%s). No metric mapping available." % (type(analytic).__name__, analytic.name))
                continue
            logging.debug("Processing %s for %s.", analytic.name, nodename)
            self.processforanalytic(nodename, analytic, mdata, reqMetrics)

        for analytic in self.firstlast:
            reqMetrics = self.metric_mapping(analytic.requiredMetrics)
            if False == reqMetrics:
                logging.info("Skipping %s (%s). No metric mapping available." % (type(analytic).__name__, analytic.name))
                continue
            logging.debug("Processing %s for %s", analytic.name, nodename)
            self.processfirstlast(nodename, analytic, mdata, reqMetrics)

    def processfirstlast(self, nodename, analytic, mdata, reqMetrics):
        # Query if timeseries exists at given timestamp
        matches = []
        start, end = self.job.start_datetime, self.job.end_datetime
        for v in reqMetrics.values():
            query = "{__name__=\"%s\","
            metric = v.split(".")
            for label in metric:
                if label is metric[0]:
                   query = query % label
                else:
                   query += (label + ',')
            query += "}"
            matches.append(query)

        available = self.timeseries_meta(start, end, matches)
        # Currently only checks if there is no data, assumes that if there is data then all timeseries are present
        if not available:
            logging.info("Skipping %s (%s). No timeseries data available." % (type(analytic).__name__, analytic.name))
            analytic.status = "failure"
            return
        
        for dt in [start, end]:
            pdata = []
            time = {'time': dt}
            for q in matches:
                # Reformat query response for plugins
                qdata = self.connect.custom_query(query=q, params=time)
                pdata.append([d['value'][1] for d in qdata])
            self.runcallback(analytic, mdata, pdata)

        analytic.status = "complete"

    def processforanalytic(self, nodename, analytic, mdata, reqMetrics):
        # Prepare query
        matches = ["{__name__=\"node_memory_MemTotal_bytes\"}", "{__name__=\"node_memory_MemFree_bytes\"}", "{__name__=\"node_memory_Cached_bytes\"}", "{__name__=\"node_memory_Slab_bytes\"}", "{__name__=\"node_cpu_seconds_total\",mode=\"user\"}"]
        start, end = self.job.start_datetime, self.job.end_datetime

        available = self.timeseries_meta(start, end, matches)
        # Currently only checks if there is no data, assumes that if there is data then all timeseries are present
        if not available:
            logging.info("Skipping %s (%s). No timeseries data available." % (type(analytic).__name__, analytic.name))
            analytic.status = "failure"
            return

        start = parse_datetime(start)
        end = parse_datetime(end)
        used = self.connect.custom_query_range(query="{__name__='node_memory_MemTotal_bytes'} - {__name__='node_memory_MemFree_bytes'}", start_time=start, end_time=end, step="30s")
        cached = self.connect.custom_query_range(query="{__name__='node_memory_Cached_bytes'}", start_time=start, end_time=end, step="30s")
        slab = self.connect.custom_query_range(query="{__name__='node_memory_Slab_bytes'}", start_time=start, end_time=end, step="30s")
        cpus = self.connect.custom_query_range(query="{__name__='node_cpu_seconds_total',mode=\"user\"}", start_time=start, end_time=end, step="30s")

        used = [x[1] for x in used[0]["values"]]
        cached = [x[1] for x in cached[0]["values"]]
        slab = [x[1] for x in slab[0]["values"]]
        cpus = [[x[1] for x in d["values"]] for d in cpus]
        cpus = [cpu for cpu in zip(*cpus)]
        
        for u, c, s, cpu in zip(used, cached, slab, cpus):
            pdata = [[u], [c], [s], cpu]
            self.runcallback(analytic, mdata, pdata)
        analytic.status = "complete"

    def runcallback(self, analytic, mdata, pdata):
        #logging.info("Running callback for %s analytic" % (analytic.name))
        callback_start = time.time()

        plugin_data = [np.array(datum, dtype=np.float) for datum in pdata]
        
        callback_time = time.time() - callback_start

        retval = analytic.process(nodemeta=mdata, data=plugin_data, timestamp=None, description=None)
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
                    print("Mapping unavailable for metric: %s" % k)
                    return False
            return mapping

    def timeseries_meta(self, start, end, matches):
        # This is basis for checking if timeseries is available
        # General form #matches.append("{__name__=%s, instance=%s}" %s (metric_name, nodename))
        #matches.append("{__name__=\"node_cpu_seconds_total\",instance=\"localhost:9100\",cpu=\"0\"}")
        #matches.append("{__name__=\"node_cpu_seconds_total\",instance=\"localhost:9100\",cpu=\"1\"}")

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
