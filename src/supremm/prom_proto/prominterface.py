import os
import logging
import urllib.parse as urlparse
import math
import json
import datetime

import numpy as np
import requests

from supremm.config import Config

HTTP_TIMEOUT = 5
CHUNK_SIZE = 4 # HOURS
MAX_DATA_POINTS = 11000 # Prometheus queries return maximum of 11,000 data points


class PromClient():
    def __init__(self, resconf):
        self._url = "{}:{}".format(resconf['prometheus_url'], resconf['prometheus_port'])
        self._step = '30s'

        self._client = requests.Session()
        self._client.mount(self._url, self._client.get_adapter(self._url))
        self._client.headers.update({'Content-Type': 'application/x-www-form-urlencoded',
                                     'Accept': 'application/json'})

    def __str__(self):
        return self._url

    def query(self, query, time):

        params = {
            'query': query,
            'time': time,
        }

        endpoint = "/api/v1/query"
        url = urlparse.urljoin(self._url, endpoint)
 
        r = self._client.get(url, params=params)
        if r.status_code != 200:
            print(str(r.content))
            return None

        return r.json()

    def query_range(self, query, start, end):

        params = {
            'query': query,
            'start': start,
            'end': end,
            'step': self._step
        }

        endpoint = "/api/v1/query_range"
        url = urlparse.urljoin(self._url, endpoint)

        r = self._client.get(url, params=params)
        if r.status_code != 200:
            print(r.content)
            return None

        return r.json()

    def ispresent(self, match, start, end):
        # This is basis for checking if timeseries is available
        # Checks if a timeseries or list of timeseries ('match[]') are available
        # without returning any actual data

        params = {
            'match[]': match,
            'start': str(start),
            'end': str(end)
        }

        endpoint = "/api/v1/series"
        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, endpoint)
        logging.debug('Prometheus QUERY SERIES META, start=%s end=%s', start, end)

        r = self._client.get(url, params=params)
        if r.status_code != 200:
            return False

        # "data" is a list of zero or more timeseries present at the specified times
        data = r.json() 
        return bool(data["data"])


    def label_val(self, match, label, start, end):
        """
        Queries label values for a corresponding metric.
        """

        params = {
            'match[]': match,
            'start': str(start),
            'end': str(end)
        }

        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, "/api/v1/label/%s/values" % label)
        logging.debug('Prometheus QUERY LABEL VALUES, start=%s end=%s', start, end)

        # Query data
        r = self._client.get(url, params=params)
        if r.status_code != 200:
            logging.error("Label Name Query Error: %s", r.content)
            return False

        data = r.json()
        names = data["data"]
        return names

    def cgroup_info(self, uid, jobid, start, end):
        """
        Queries a job's cgroup
        """

        match = "cgroup_info{uid='%s',jobid='%s'}" % (uid, jobid)

        params = {
            'match[]': match,
            'start': str(start),
            'end': str(end)
        }

        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, "/api/v1/label/cgroup/values")
        logging.debug('Prometheus QUERY CGROUP, start=%s end=%s', start, end)

        # Query data
        r = self._client.get(url, params=params)
        if r.status_code != 200:
            logging.error("Cgroup Query Error: %s", r.content)
            return False

        data = r.json()

        # TODO should handle no cgroup info more gracefully
        if len(data["data"]) == 0:
            logging.warning("No Cgroup info available.")
            return None

        return data["data"][0]


class Context():
    def __init__(self, start, end, client):
        self.start = start
        self.end = end
        self.client = client

        self.reqMetrics = None
        self.timestamp = start
        self.min_ts = start
        self.next_min_ts = np.inf
        self._result = None
        self._idx_dict = {}

    def __str__(self):
        return str(self._idx_dict)

    @property
    def mode(self):
        return self._mode

    @mode.setter
    def mode(self, m):
        self._mode = m

    @property
    def reqMetrics(self):
        return self._reqMetrics

    @reqMetrics.setter
    def reqMetrics(self, rm):
        self._reqMetrics = rm

    @property
    def min_ts(self):
        return self._min_ts

    @min_ts.setter
    def min_ts(self, ts):
        self._min_ts = ts

    @property
    def next_min_ts(self):
        return self._next_min_ts

    @next_min_ts.setter
    def next_min_ts(self, ts):
        self._next_min_ts = ts

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, ts):
        self._timestamp = ts

    def fetch(self, required_metrics):

        self.reqMetrics = required_metrics
        self.init_internal_state()
        if self.mode == "all" or self.mode == "timeseries":
            for start, end in self.chunk_timerange():
                yield [self.client.query_range(m.apply_scaling(), start, end) for m in required_metrics]
                self.reset_internal_state()

        elif self.mode == "firstlast":
            for ts in (self.start, self.end):
                yield [self.client.query(m.apply_scaling(), ts) for m in required_metrics]
                self.reset_internal_state()

    def chunk_timerange(self):
        """
        Generator function to return chunked time ranges for a job of arbitrary length.
        """
        chunk_start = datetime.datetime.fromtimestamp(self.start)

        if (self.end - self.start) < (CHUNK_SIZE * 60 * 60):
            yield self.start, self.end
            return

        done = False
        while not done:
            chunk_end = chunk_start + datetime.timedelta(hours=CHUNK_SIZE)
            if chunk_end.timestamp() > self.end:
                yield chunk_start.timestamp(), self.end
                done = True
            yield chunk_start.timestamp(), chunk_end.timestamp()
            chunk_start = chunk_end

    def extractpreproc_values(self, result):

        if self.mode == "all" or self.mode == "timeseries":
            for data, description in self.formatmatrixpreproc(result):
                yield data, description

        elif self.mode == "firstlast":
            for data, description in self.formatvectorpreproc(result):
                yield data, description

    def extract_values(self, result):

        if self.mode == "all" or self.mode == "timeseries":
            for data, description in self.formatmatrix(result):
                yield data, description

        elif self.mode == "firstlast":
            for data, description in self.formatvector(result):
                yield data, description

    def getdescriptions(self, result, type, fmt):

        metric_ids = {idx: metric for idx, metric in enumerate(self.reqMetrics)}

        descriptions = []
        for idx, datum in enumerate(result):
            mmap = metric_ids[idx]
            groupby = mmap.groupby
            outfmt = mmap.outformat

            metric_descriptions = []
            for inst in datum["data"]["result"]:
                id = inst["metric"][groupby]

                # Vectors and matrices have different "value(s)" keys
                if type == "vector":
                    min_ts = inst["value"][0]
                elif type == "matrix":
                    min_ts = inst["values"][0][0]
                self.min_ts = min(self.min_ts, min_ts)

                if mmap.outformat == groupby:
                    metric_descriptions.append(inst["metric"][groupby])
                    self.add_instance(idx, id, min_ts)
                else:
                    outstring = outfmt[0]
                    args = outfmt[1:]
                    out = []
                    for arg in args:
                        out.append(inst["metric"][arg])
                    try:
                        metric_descriptions.append(outstring.format(*out))
                        self.add_instance(idx, id, min_ts)
                    except TypeError:
                        logging.warning("Unable to format configured outstring %s with args: %s", outstring, args)
                        return None

            if fmt == "analytic":
                descriptions.append( (np.arange(0, len(metric_descriptions)), metric_descriptions) )
            elif fmt == "preproc":
                descriptions.append({idx: md for idx, md in enumerate(metric_descriptions)})

        return descriptions

    def formatvectorpreproc(self, result):
        """

        """
        description = self.getdescriptions(result, "vector", "preproc")
        if not description:
            yield None, None

        data = []
        for datum in result:
            size = len(datum["data"]["result"])
            indices = np.arange(size)
            vals = np.fromiter(self.populatevector(datum), np.float64, size)
            data.append(np.column_stack((vals, indices)))

        yield data, description

    def formatvector(self, result):
        """

        """
        description = self.getdescriptions(result, "vector", "analytic")
        if not description:
            yield None, None 

        data = []
        for datum in result:
            size = len(datum["data"]["result"])
            data.append(np.fromiter(self.populatevector(datum), np.float64, size))

        yield data, description

    def formatmatrixpreproc(self, result):
        """

        """
        description = self.getdescriptions(result, "matrix", "preproc")
        if not description:
            yield None, None 

        # Populate data array to pass to preproc
        done = False
        while not done:
            data = []
            for idx, datum in enumerate(result):
                size = len(datum["data"]["result"])
                indices = np.arange(size)
                vals = np.fromiter(self.populatematrix(idx, datum), np.float64, size)
                data.append(np.column_stack((vals, indices)))

            self.update_min_ts()
            if np.inf == self.min_ts:
                done = True

            yield data, description

    def formatmatrix(self, result):
        """
        """
        description = self.getdescriptions(result, "matrix", "analytic")
        if not description:
            yield None, None

        done = False
        while not done:
            data = []
            for idx, datum in enumerate(result):
                size = len(datum["data"]["result"])
                data.append(np.fromiter(self.populatematrix(idx, datum), np.float64, size))

            min_ts = self.min_ts
            self.update_min_ts()
            if np.inf == self.min_ts:
                done = True

            yield data, description

    def populatevector(self, data):
        """ Generator to populate numpy array 
            from prometheus vector resultType
        """
        for inst in data["data"]["result"]:
            yield inst["value"][1]

    def populatematrix(self, metric_idx, data):
        """ Generator to populate numpy array 
            from prometheus matrix resultType
        """
        mmap = self.reqMetrics[metric_idx]
        groupby = mmap.groupby

        for inst in data["data"]["result"]:
            id = inst["metric"][groupby]
            idx = self.get_idx(metric_idx, id)
            try:
                ts = inst["values"][idx][0]
            except IndexError:
                yield np.NaN
                continue

            if ts == self.min_ts:
                value = inst["values"][idx][1]
                try:
                    next_ts = inst["values"][idx+1][0]
                except IndexError:
                    next_ts = np.inf

                self.next_min_ts = min(self.next_min_ts, next_ts)
                self.update_inst_ts(metric_idx, id, next_ts)
                yield value

            else:
                yield np.NaN

    def init_internal_state(self):
        self._idx_dict.update({midx : {} for midx, _ in enumerate(self.reqMetrics)})

    def reset_internal_state(self):
        for metric_idx, val in self._idx_dict.items():
            for k, inst in val.items():
                self._idx_dict[metric_idx][k] = {"idx" : 0, "ts" : np.inf}

    def instance_count(self, metric):
        return len(self._idx_dict[metric]["inst"].keys())        

    def update_inst_ts(self, metric, inst, ts):
        self._idx_dict[metric][inst]["ts"] = ts
        self._idx_dict[metric][inst]["idx"] += 1

    def update_min_ts(self):
        # Save min_ts to timestamp attribute that is
        # used externally when sent to the plugin callback
        self.timestamp = self.min_ts
        self.min_ts = self.next_min_ts
        self.next_min_ts = np.inf

    def get_label(self, metric):
        return self._idx_dict[metric]["label"]

    def get_idx(self, metric_idx, inst):
        return self._idx_dict[metric_idx][inst]["idx"]

    def get_ts(self, metric, inst):
        return self._idx_dict[metric]["insts"][inst]["ts"]

    def add_metric(self, metric, label):
        self._idx_dict.update({metric : {"insts" : {}, "label" : label}})

    def add_instance(self, metric_idx, inst, ts):
        idx_dict = {"idx" : 0, "ts" : ts}
        self._idx_dict[metric_idx].update({inst : idx_dict})
        self.min_ts = min(self.min_ts, ts)
