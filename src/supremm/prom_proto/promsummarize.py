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

from prominterface import PromClient, formatforplugin # Use local import for debugging, testing
from supremm.plugin import loadpreprocessors, loadplugins, NodeMetadata
from supremm.rangechange import RangeChange


VERSION = "2.0.0"
MAX_CHUNK = 24 * 3

def load_translation():
    """
    Update mapping of available Prometheus metrics
    with corresponding PCP metric names.
    """        
    # Load mapping
    prom2pcp = {}
    version = "v5"
    file = "mapping/%s.json" % (version)
    file_path = os.path.abspath(file)
    with open(file_path, "r") as f:
        prom2pcp = json.load(f)

    logging.debug("Available metric mapping(s): \n{}".format(json.dumps(prom2pcp, indent=2)))
    return prom2pcp        

class NodeMeta(NodeMetadata):
    """ container for node metadata """
    def __init__(self, nodename, idx):
        self._nodename = nodename
        self._nodeidx = idx
        self._archivedata = None

    nodename = property(lambda self: self._nodename)
    nodeindex = property(lambda self: self._nodeidx)
    archive = property(lambda self: self._archivedata)

class PromSummarize():
    def __init__(self, preprocessors, analytics, job):
        # Establish connection with server:
        self.url = "http://127.0.0.1:9090"
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

        # TODO use config query to parse yaml scrape interval config
        self.timestep = "30s"

        # TODO this is set from opts/configs NOT hardcoded
        self.fail_fast = True

    def get(self):
        # TODO this should inherit from an abstract Summarize class
        # Data at this point are in the same format from preprocs/plugins
        # and therefore should be processed the same regardless of backend config.
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
        self.nodes_processed = 0

        for nodename in self.job.nodenames():
            try:
                logging.info("Summarizing job %s on node %s", self.job, nodename)
                idx = self.nodes_processed
                self.processnode(nodename, idx)
                self.nodes_processed += 1

            except Exception as exc:
                print("Something went wrong. Oops!")
                success -= 1
                # TODO add code for self.adderror
                #self.adderror("node", "Exception {0} for node: {1}".format(exc, nodename))
                if self.fail_fast:
                    raise

        return success == 0

    def processnode(self, nodename, idx):
        # Create metadata from nodename
        mdata = NodeMeta(nodename, idx)

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
        # Serialize as unix timestamp here for prometheus metadata queries.
        # Later on in this function the self.job.(start/end)_datetime python objects
        # are used for chunking. Maybe determine chunks first (if necessary)? Include job's (start, end) then list of chunked (start, end)
        start_ts, end_ts = self.job.start_datetime.timestamp(), self.job.end_datetime.timestamp()
        preproc.hoststart(mdata.nodename)
        logging.debug("Processing %s (%s)" % (type(preproc).__name__, preproc.name))
         
        metrics = []
        descriptions = []
        for m in reqMetrics.values():
            if preproc.name == "procprom":
                cgroup = self.client.cgroup_info(self.job.acct['uid'], self.job.job_id, start_ts, end_ts)
                metric = m['metric'] % (cgroup, mdata.nodename)
            else:
                metric = m['metric'] % mdata.nodename

            base = metric.split()[0]

            # Check if timeseries is available
            available = self.client.timeseries_meta(start_ts, end_ts, base)
            if not available:
                logging.warning("Skipping %s (%s). No data available." % (type(preproc).__name__, preproc.name))
                preproc.hostend()
                return

            # Get metric label -> description from metric and label
            label = m['label']
            description = self.client.label_val_meta(start_ts, end_ts, base, m['label'], 'preprocessor')
            metrics.append(metric)
            descriptions.append(description)

        for start, end in chunk_timerange(self.job.start_datetime, self.job.end_datetime):
            pdata = [self.client.query_range(metric, start, end, 'preprocessor') for metric in metrics]
            if False == self.runpreproccall(preproc, mdata, pdata, descriptions):
                break

        preproc.status = "complete"
        preproc.hostend()

    def processfirstlast(self, analytic, mdata, reqMetrics):
        start, end = self.job.start_datetime.timestamp(), self.job.end_datetime.timestamp()
        logging.debug("Processing %s (%s)" % (type(analytic).__name__, analytic.name))

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
            rdata = [self.client.query(m, ts) for m in metrics]
            self.runcallback(analytic, mdata, rdata, descriptions)
 
        analytic.status = "complete"

    def processforanalytic(self, nodename, analytic, mdata, reqMetrics):
        start, end = self.job.start_datetime.timestamp(), self.job.end_datetime.timestamp()
        logging.debug("Processing %s (%s)" % (type(analytic).__name__, analytic.name))

        data = []
        descriptions = []
        metrics = []
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

        for start, end in chunk_timerange(self.job.start_datetime, self.job.end_datetime):
            rdata = [self.client.query_range(m, start, end) for m in metrics]
            if False == self.runcallback(analytic, mdata, rdata, descriptions):        
                break

        analytic.status = "complete"

    def runpreproccall(self, preproc, mdata, rdata, descriptions):
        """ Call the pre-processor data processing function 
        """
        ts = 0
        retval = preproc.process(timestamp=ts, data=rdata, description=descriptions)
        return retval

    def runcallback(self, analytic, mdata, rdata, description):
        """ Call the plugin data processing function """
        format_func = formatforplugin(rdata)

        # Initialize minimum timestamp with first available timestamp
        # Note: 'label' is not universal for required metrics so this should
        # be moved out of 'context'.

        init_ctx = {
                "ts_min" : int(rdata[0]["data"]["result"][0]["values"][0][0]),
                "label" : "host",
                "idx_dict" : dict()
        }

        for m in rdata:
            name = m["data"]["result"][0]["metric"]["__name__"]
            metrics = { name : {} }
            for inst in m["data"]["result"]:
                ### 'Label' is uniqueness
                inst_id = inst["metric"][init_ctx["label"]]
                ts = int(inst["values"][0][0])
                if ts < init_ctx["ts_min"]:
                    ctx["ts_min"] = ts
                metrics[name].update({inst_id : {"idx" : 0, "ts" : ts}})
            init_ctx["idx_dict"].update(metrics)

        # Size of numpy array = total number of instances from all responses
        ctx = init_ctx
        while True:
            try:
                next(format_func)
                ts, vals, new_ctx = format_func.send(ctx)
                ctx = new_ctx
            except StopIteration:
                break
            try:
                retval = analytic.process(mdata, ts, vals, description)
                if not retval:
                    break
            except Exception as e:
                print(e)
                break

        return False

    def metric_mapping(self, reqMetrics):
        """
        Recursively checks if a mapping is available from a given metrics list or list of lists.

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
            for k in mapping.keys():
                try:
                    mapping[k] = self.valid_metrics[k]
                    # Prometheus-only plugins/preprocs
                    if k[:5] == "prom:":
                        full_metric = mapping[k]['metric']
                        if k[5:] not in full_metric:
                            mapping[k]['metric'] = k[5:] + full_metric
                        else:
                            mapping[k]['metric'] = full_metric
                except KeyError:
                    logging.warning("Mapping unavailable for metric: %s", k)
                    return False
            return mapping

def chunk_timerange(job_start, job_end):
    """
    Generator function to return chunked time ranges for a job of arbitrary length.
    This is necessary due to Prometheus's hard-coded limit of 11,000 data points:
    https://github.com/prometheus/prometheus/blob/30af47535d4d7c0a7566df78e63e77515ba26863/web/api/v1/api.go#L202

    params: job_start, job_end - Python datetime objects of a job's start and times
    yield: chunk_start, chunk_end - Python datetime objects of a given chunk's start and end times

    'chunk_end' will be the 'job_end' for the final chunk less than the maximum specified chunk size.
    """

    chunk_start = job_start
    while True:
        chunk_end = chunk_start + datetime.timedelta(hours=MAX_CHUNK)
        if chunk_end > job_end:
            yield chunk_start.timestamp(), job_end.timestamp()
            break
        yield chunk_start.timestamp(), chunk_end.timestamp()
        chunk_start = chunk_end
