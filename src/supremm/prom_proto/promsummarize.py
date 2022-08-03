import sys
import os
import json
import time
import logging
import requests
import inspect
import datetime
import urllib.parse as urlparse
from collections import OrderedDict

import requests
import numpy as np

from supremm.prom_proto.prominterface import PromClient
from supremm.plugin import loadpreprocessors, loadplugins, NodeMetadata
from supremm.rangechange import RangeChange

VERSION = "2.0.0"

def load_translation():
    """
    Update mapping of available Prometheus metrics
    with corresponding PCP metric names.
    """        
    # Load mapping
    prom2pcp = {}
    version = "v5"
    file = "prom_proto/mapping/%s.json" % (version)
    file_path = os.path.abspath(file)
    with open(file_path, "r") as f:
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

        # TODO this is set from opts/configs NOT hardcoded
        self.fail_fast = True

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

        output['summarization'] = {
            "version": VERSION,
            "elapsed": time.time() - self.start,
            "created": time.time(),
            "srcdir": self.job.jobdir,
            "complete": True} # True for now

        output['created'] = datetime.datetime.utcnow()

        output['acct'] = self.job.acct
        output['acct']['id'] = self.job.job_id

        return output

    def process(self):
        """ Main entry point. All nodes are processed. """
        success = 0
        self.archives_processed = 0

        for nodename in self.job.nodenames():
            try:
                print("Processing node: %s" % nodename)
                logging.info("Summarizing job %s on node %s", self.job, nodename)
                self.processnode(nodename)
                self.nodes_processed += 1

            except Exception as exc:
                print("Something went wrong. Oops!")
                success -= 1
                # TODO add code for self.adderror
                #self.adderror("node", "Exception {0} for node: {1}".format(exc, nodename))
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
            self.processfirstlast(analytic, mdata, reqMetrics)

    def processforpreproc(self, mdata, preproc, reqMetrics):
        start, end = self.job.start_datetime.timestamp(), self.job.end_datetime.timestamp()
        preproc.hoststart(mdata.nodename)
 
        metrics = []
        descriptions = []
        for m in reqMetrics.values():
            metric = m['metric'] % mdata.nodename
            base = metric.split()[0]

            # Check if timeseries is available
            available = self.client.timeseries_meta(start, end, base)
            if not available:
                logging.warning("Skipping %s (%s). No data available." % (type(preproc).__name__, preproc.name))
                preproc.status = "failure"
                preproc.hostend()
                return

            # Get metric label -> description from metric and label
            label = m['label']
            description = self.client.label_val_meta(start, end, base, m['label'], 'preprocessor')
            metrics.append(metric)
            descriptions.append(description)

        # TODO use config query to parse yaml scrape config
        rdata = [self.client.query(metric, start, 'preprocessor') for metric in metrics]
        while True:
            if False == self.runpreproccall(preproc, mdata, rdata, start, descriptions):
                break

            start += 30 # HARD-CODED TIMESTEP
            if start > end:
                preproc.status = "failure"
                preproc.hostend()
                return

        preproc.status = "complete"
        preproc.hostend()

    def processfirstlast(self, analytic, mdata, reqMetrics):
        start, end = self.job.start_datetime.timestamp(), self.job.end_datetime.timestamp()
        """
        OLD
        matches = [x['metric'].split()[0] % mdata.nodename for x in reqMetrics.values()]

        for match in matches:
            available = self.client.timeseries_meta(start, end, match)
            if not available:
                logging.warning("Skipping %s (%s). No timeseries present." % (type(analytic).__name__, analytic.name))
                analytic.status = "failure"
                return

        # TODO add scale factor in here -> pass as parameter to client's query OR just scale response array at the end
        label = set(x['label'] for x in reqMetrics.values()).pop()

        # TODO something fishy here ... why loop over matches then just pass matches[] to function anyway?
        description = np.asarray([self.client.label_val_meta(start, end, matches, label, 'plugin') for m in matches])

        for ts in (start, end):
            self.runcallback(analytic, mdata, matches, ts, description)
        """

        metrics = []
        descriptions = []
        for m in reqMetrics.values():
            metric = m['metric'] % mdata.nodename
            base = metric.split()[0]

            # Check if timeseries is available
            available = self.client.timeseries_meta(start, end, base)
            if not available:
                logging.warning("Skipping %s (%s). No data available." % (type(analytic).__name__, analytic.name))
                analytic.status = "failure"
                return

            # Get metric label -> description from metric and label
            label = m['label']
            description = self.client.label_val_meta(start, end, base, m['label'], 'plugin')
            metrics.append(metric)
            descriptions.append(description)

        for ts in (start, end):
            self.runcallback(analytic, mdata, metrics, ts, descriptions)
 
        analytic.status = "complete"

    def processforanalytic(self, nodename, analytic, mdata, reqMetrics):
        return
        start, end = self.job.start_datetime.timestamp(), self.job.end_datetime.timestamp()
        matches = [x['metric'] % mdata.nodename for x in reqMetrics.values()]

        for m in matches:
            available = self.timeseries_meta(start, end, matches)
            if not available:
                logging.warning("Skipping %s (%s). No data available." % (type(analytic).__name__, analytic.name))
                analytic.status = "failure"
                return

        # TODO add scale factor in here -> pass as parameter to client's query OR just scale response array at the end
        l = set(x['label'] for x in reqMetrics.values()).pop()

        # TODO something fishy here ... why loop over matches then just pass matches[] to function anyway?
        description = np.asarray([self.client.label_val_meta(start, end, matches, l) for m in matches])

        # TODO parse configuration setting
        timestep = "30s"

        # query data from time range 
        while not done:
            done = True

        #OLD
        #rdata = [self.connect.custom_query_range(metric['metric'], start, end, timestep, 'plugin') for metric in reqMetrics.values()]
        #for ts, d in formatforplugin(rdata, "matrix"):
        #    self.runcallback(analytic, mdata, d, ts, description)
        
        analytic.status = "complete"

    def runpreproccall(self, preproc, mdata, data, ts, description):
        """ Call the pre-processor data processing function 
        """
        """
        OLD
        data = []
        if len(pdata[0]) == 1:
            data.append([[float(pdata[0][0]),-1]])
        else:
            for m in pdata:
                datum = []
                for idx, v in enumerate(m):
                    datum.append((float(v), idx))
                data = [datum]
        """
        retval = preproc.process(timestamp=ts, data=data, description=description)
        return retval

    def runcallback(self, analytic, mdata, matches, ts, description):
        """ Call the plugin data processing function """
        # TODO handle vectors and matrices differently OR handle timeslices
        try:
            plugin_data = [self.client.query(m, ts, 'plugin') for m in matches]
            retval = analytic.process(nodemeta=mdata, timestamp=ts, data=plugin_data, description=description)
            return retval
        except Exception as exc:
            logging.error("An error occurred with the query: %s", exc)

        # OLD plugin_data = [np.array(datum, dtype=np.float) for datum in pdata]

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
                #TODO add code here to process prometheus only metrics, format same as pcp->prom mapping
                if k[:4] == "prom:":
                    v['metric'] += k[4:] # remove "prom:"
                try:
                    mapping[k] = self.valid_metrics[k]
                except KeyError:
                    logging.warning("Mapping unavailable for metric: %s", k)
                    return False
            return mapping
