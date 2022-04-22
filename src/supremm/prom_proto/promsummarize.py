import os
import json
import time
import logging
import requests
import urllib.parse as urlparse

import numpy as np
import prometheus_api_client as pac
from prometheus_api_client.utils import parse_datetime

from supremm.proc_common import filter_plugins, instantiatePlugins
from supremm.plugin import loadpreprocessors, loadplugins, NodeMetadata


class NodeMeta(NodeMetadata):
    """ container for node metadata """
    def __init__(self, nodename):
        self._nodename = nodename
        self._nodeidx = None
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
        self.available_metrics = self.connect.all_metrics()
        self.valid_metrics = {}
        self.translate()

        # Standard summarization attributes
        self.analytics = analytics
        self.firstlast = [x for x in analytics if x.mode == "firstlast"]
        self.job = job

    def series_meta(self, start, end):
        # TODO this is basis for checking if timeseries is available
        # TODO build 'match[]' for node from metric, instance
        # Manually add series selectors for now, but programatically generate soon
        matches = []
        # General form #matches.append("{__name__=%s, instance=%s}" %s (metric_name, nodename))
        matches.append("{__name__=\"node_cpu_seconds_total\",instance=\"localhost:9100\",cpu=\"0\"}") 
        matches.append("{__name__=\"node_cpu_seconds_total\",instance=\"localhost:9100\",cpu=\"1\"}") 

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
        logging.debug('Prometheus QUERY SERIES META, url="%s" params="%s"', url, params)
        r = requests.post(url, data=params, headers=headers)
        if r.status_code != 200:
            return False 
        data = r.json()
        # data is a list of valid queries to pass along elsewhere
        return data["data"]

    def translate(self):
        """
        Update mapping of available Prometheus metrics
        with corresponding PCP metric names.

        This should be called per-node as different nodes
        could export different metrics.
        """
        
        # Load mapping
        prom2pcp = {}
        with open("prom2pcp.json") as f:
            prom2pcp = json.load(f)
        
        for plugin, map in prom2pcp.items():
            prom_mn, pcp_mn = map[0], map[1]

            if prom_mn in self.available_metrics:          
                self.valid_metrics.update({plugin : (prom_mn, pcp_mn)})
                logging.debug("%i PCP plugin metric(s) available from %s", len(pcp_mn), prom_mn)  
        logging.debug("Available metric mapping(s): \n{}".format(json.dumps(self.valid_metrics, indent=2))) 

    def get(self):
        logging.info("Returning summary information")
        output = {}

        for analytic in self.firstlast:
            output[analytic.name] = analytic.results()

        return output

    def process(self):
        # Call equivalent of processarchive for every node in job's nodelist

        # For now just a single node
        nodelist = ["prometheus-dev.ccr.xdmod.org"]
        for nodename in nodelist:
            logging.info("Processing node %s", nodename)
            node_proc_start = time.time()

            # Build node/job context
            # Context:
            #    - node.instance
            #    - job.start
            #    - job.end
            # Build timeseries 'match[]' from context
            # Pass 'match[]' to process_node() for metric translation -> 
            #self.series_meta(self.job.start_datetime, self.job.end_datetime)

            self.process_node(nodename)

            node_proc_time = time.time() - node_proc_start
            logging.debug("%s summarized in %s seconds" % (nodename, node_proc_time))

    def process_node(self, nodename):
        # Create metadata from nodename
        mdata = NodeMeta(nodename)

        # Build series to query 

        # Select between preprocs, plugins:"all", plugins:"firstlast"
        for analytic in self.firstlast:
            # Only process if the timeseries for requiredMetrics are available
            for item in analytic.requiredMetrics:
                continue

            logging.debug("Processing %s for %s", analytic.name, nodename)
            self.processfirstlast(nodename, analytic, mdata)

    def processfirstlast(self, nodename, analytic, mdata):
        # Check if required metrics are available
        # Query if timeseries exists at given timestamp
        # http://172.22.0.216:9090/api/v1/series?start=&end=' --data-urlencode 'match[]=prom_metric_name{label="labelname"}'
        if analytic.name not in self.valid_metrics:
            logging.debug("Skipping %s (%s)" % (type(analytic).__name__, analytic.name))
            return

        self.runcallback(analytic, mdata)

    def runcallback(self, analytic, mdata):
        logging.info("Running callback for %s analytic" % (analytic.name))
        query_start = time.time()
        ##########################################
        ################ Grab data from prometheus 
        # Query configs
        prom_metric_name = "node_cpu_seconds_total"
        label_config = {"cpu": "1"}
        start_params = {'time': '2022-04-18T12:38:00.000Z'} #{'time': '2022-04-05T09:30:00.781Z'}
        end_params = {'time': '2022-04-18T12:43:00.000Z'} #{'time': '2022-04-05T09:35:00.781Z'} 

        # Grab data -> [first], [last] -> [first, last] -> <type MetricsList>
        #first = self.connect.get_current_metric_value(prom_metric_name, label_config, start_params)
        #last = self.connect.get_current_metric_value(prom_metric_name, label_config, end_params)

        first = self.connect.custom_query(query="node_cpu_seconds_total", params=start_params)       
        last = self.connect.custom_query(query="node_cpu_seconds_total", params=end_params)

        ################ Reformat for plugins
        first_data = {}
        for item in first:
            mode = item['metric']['mode']
            if mode == "steal":
                continue

            if mode not in first_data.keys():
                first_data.update({mode: [item['value'][1]]})

            else:
                first_data[mode].append(item['value'][1])

        last_data = {}
        for item in last:
            mode = item['metric']['mode']
            if mode == "steal":
                continue

            if mode not in last_data.keys():
                last_data.update({mode: [item['value'][1]]})

            else:
                last_data[mode].append(item['value'][1])  

        first = [x for x in first_data.values()]
        first = np.array(first)#, dtype=np.uint64)
        first = first.astype(np.float) 

        last = [x for x in last_data.values()]
        last = np.array(last)#, dtype=np.uint64)  
        last = last.astype(np.float)
        #############################################
        query_time = time.time() - query_start
        logging.debug("Query executed in {} seconds".format(query_time)) # This includes time to reformat

        retval = analytic.process(nodemeta=mdata, data=first, timestamp=None, description=None)
        retval = analytic.process(nodemeta=mdata, data=last, timestamp=None, description=None)
        return retval
