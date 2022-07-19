import sys
import os
import json
import time
import logging
import urllib.parse as urlparse
from collections import OrderedDict

import requests
import numpy as np

from supremm.proc_common import filter_plugins, instantiatePlugins
from supremm.prom_proto.prominterface import PromClient
from supremm.plugin import loadpreprocessors, loadplugins, NodeMetadata
from supremm.rangechange import RangeChange


def load_translation():
    """
    Update mapping of available Prometheus metrics
    with corresponding PCP metric names.
    """        
    # Load mapping
    prom2pcp = {}
    version = "v4"
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
        self.client = PromClient(url=self.url)

        # Translation Prom -> PCP metric names
        self.valid_metrics = load_translation()

        # Standard summarization attributes
        self.preprocs = preprocessors
        self.firstlast = [x for x in analytics if x.mode == "firstlast"]
        self.alltimestamps = [x for x in analytics if x.mode in ("all", "timeseries")]
        self.errors = {} 
        self.job = job
        self.start = time.time()
        self.nodes_processed = 0

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
        """ Main entry point. All nodes are processed. """
        success = 0
        self.archives_processed = 0

        for nodename in self.job.nodenames():
            print(nodename)
            try:
                print("Summarizing job {0} on node {1}".format(self.job, nodename))
                self.processnode(nodename)
                self.nodes_processed += 1

            except Exception as exc:
                print("Something went wrong. Oops!")
                success -= 1
                # TODO add code for self.adderror
                self.adderror("node", "Exception {0} for node: {1}".format(exc, nodename))
                if self.fail_fast:
                    raise

        return success == 0

    def processnode(self, nodename):
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
        start, end = self.job.nodestart, self.job.end_datetime

        #available = self.timeseries_meta(start, end, reqMetrics.values())
        ## Currently only checks if there is no data, assumes that if there is data then all timeseries are present
        #if not available:
        #    logging.warning("Skipping %s (%s). No data available." % (type(preproc).__name__, preproc.name))
        #    preproc.status = "failure"
        #    preproc.hostend()
        #    return
        preproc.hoststart(mdata.nodename)

        matches = [x['metric'].split()[0] for x in reqMetrics.values()]
        l = set(x['label'] for x in reqMetrics.values()).pop()
        description = np.asarray([self.client.label_val_meta(start, end, matches, l) for m in matches])
        
        start = parse_datetime(start)
        end = parse_datetime(end) 
        timestep = "30s"
 
        rdata = [self.connect.custom_query_range(metric['metric'], start, end, timestep) for metric in reqMetrics.values()]
        for ts, d in formatforplugin(rdata, "matrix"):
            if False == self.runpreproccall(preproc, mdata, d, ts, description):
                break
        
        preproc.status = "complete"
        preproc.hostend()

    def processfirstlast(self, nodename, analytic, mdata, reqMetrics):
        # Query if timeseries exists at given timestamp
        start, end = self.job.start_datetime, self.job.end_datetime

        # TODO update metric mapping before this can be done
        #available = self.timeseries_meta(start, end, reqMetrics.values())
        ## Currently only checks if there is no data, assumes that if there is data then all timeseries are present
        #if not available:
        #    logging.warning("Skipping %s (%s). No data available." % (type(analytic).__name__, analytic.name))
        #    analytic.status = "failure"
        #    return

        start = datetime_to_timestamp(start)
        end = datetime_to_timestamp(end)

        matches = [x['metric'].split()[0] for x in reqMetrics.values()]
        l = set(x['label'] for x in reqMetrics.values()).pop()
        description = np.asarray([self.client.label_val_meta(start, end, matches, l) for m in matches])

        for t in (start, end):
            rdata = [self.client.query(m, t) for m in matches]
            assert len(rdata) == len(description)
            #ts = rdata[0][0]
            #pdata = [d[:-1] for d in rdata]
            print(*rdata)
            sys.exit(0)       

            self.runcallback(analytic, mdata, pdata, ts, description)

        analytic.status = "complete"

    def processforanalytic(self, nodename, analytic, mdata, reqMetrics):
        start, end = self.job.start_datetime, self.job.end_datetime

        #available = self.timeseries_meta(start, end, reqMetrics.values())
        # Currently only checks if there is no data, assumes that if there is data then all timeseries are present
        #if not available:
        #    logging.warning("Skipping %s (%s). No data available." % (type(analytic).__name__, analytic.name))
        #    analytic.status = "failure"
        #    return

        matches = [x['metric'].split()[0] for x in reqMetrics.values()]
        l = set(x['label'] for x in reqMetrics.values()).pop()
        description = np.asarray([self.client.label_val_meta(start, end, matches, l) for m in matches])

        start = parse_datetime(start)
        end = parse_datetime(end)
        timestep = "30s"
        
        rdata = [self.connect.custom_query_range(metric['metric'], start, end, timestep) for metric in reqMetrics.values()]
        for ts, d in formatforplugin(rdata, "matrix"):
            self.runcallback(analytic, mdata, d, ts, description)
        
        analytic.status = "complete"

    def runpreproccall(self, preproc, mdata, pdata, ts, description):
        """ Call the pre-processor data processing function 
        """
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
        retval = preproc.process(timestamp=ts, data=preproc_data, description=description)
        return retval

    def runcallback(self, analytic, mdata, pdata, ts, description):
        """ Call the plugin data processing function """
        callback_start = time.time()
        plugin_data = [np.array(datum, dtype=np.float) for datum in pdata]
        retval = analytic.process(nodemeta=mdata, timestamp=ts, data=plugin_data, description=description)

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
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            return False

        data = r.json()
        # data is a list of valid queries to pass along elsewhere
        return data["data"]
