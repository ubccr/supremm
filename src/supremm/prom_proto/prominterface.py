import logging
import urllib.parse as urlparse
from sys import getsizeof

import numpy as np
import requests


HTTP_TIMEOUT = 5

MAX_DATA_POINTS = 11000 # prometheus queries return max of 11,000 data points

class PromClient():

    def __init__(self, url):
        self._url = url

    def __str__(self):
        return self._url

    def query(self, query, time, type):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        params = {
            'query': query,
            'time': time,
        }

        endpoint = "/api/v1/query"
        url = urlparse.urljoin(self._url, endpoint)
 
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            print(str(r.content))
            return None

        data = r.json()
        if type == 'preprocessor':
            pdata = formatforpreproc(data)
        elif type == 'plugin':
            pdata = formatforplugin(data)
        return pdata

    def query_range(self, query, start, end, step="30s"):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        params = {
            'query': query,
            'start': start,
            'end': end,
            'step': step
        }

        endpoint = "/api/v1/query_range"
        url = urlparse.urljoin(self._url, endpoint)
        
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            print(r.content)
            return None
        
        data = r.json()
        #print(len(data['data']['result'][0]['values'])) # DEBUG
        #print(getsizeof(data['data']['result'][0]['values'] * 4)) #DEBUG
        
        plugin_data = formatforplugin(data)
        return plugin_data

    def timeseries_meta(self, start, end, match):
        # This is basis for checking if timeseries is available
        # Checks if a timeseries or list of timeseries ('match[]') are available
        # without returning any actual data
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        params = {
            'match[]': match,
            'start': str(start),
            'end': str(end)
        }

        endpoint = "/api/v1/series"
        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, endpoint)
        logging.debug('Prometheus QUERY SERIES META, url=%s start=%s end=%s', url, start, end)
        
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            return False

        data = r.json()
        # data is a list of timeseries present at the given specified times
        
        return bool(data["data"])


    def label_val_meta(self, start, end, matches, label, type):
        """
        Queries label values for a given metric.
        """
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        params = {
            'match[]': matches,
            'start': str(start),
            'end': str(end)
        }

        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, "/api/v1/label/%s/values" % label)
        logging.debug('Prometheus QUERY LABEL VALUES, url=%s start=%s end=%s', url, start, end)

        # Query data
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            logging.error("Label Name Query Error: %s", r.content)
            return False
        data = r.json()
        names = data["data"]
        label_idx = np.arange(0, len(names))

        # Format for preprocessor
        if type == "preprocessor":
            description = dict(zip(label_idx, names))
            return description #[label_idx, names]

        # Format for plugin
        elif type == "plugin":
            description = (label_idx, names)
            return description #[label_idx, names]

def formatforplugin(rdata):
    """
    Format Prometheus query response into the expected format for SUPReMM plugins.
    Check out https://prometheus.io/docs/prometheus/latest/querying/api/ for the formatting information.
    
    params: Prometheus json response
    return: numpy array, dtype=uint64
            matrices: [
                       [inst0 inst1 ... instN],
                       [inst0 inst1 ... instN],
                       ...
                       [inst0 inst1 ... instN]
                      ]
            vectors: [inst0 inst1 ... instN]
    """
    rtype = rdata["data"]["resultType"]
    result = rdata["data"]["result"]

    # Process vector
    if rtype == "vector":
        # Allocate numpy array with shape (1, instances)
        # A vector only corresponds to one timestamp
        instances = len(result)
        size = instances

        # Format data
        data = np.fromiter(formatvector(result), dtype=np.uint64, count=size)
        return data

    # Process matrix
    elif rtype == "matrix":
        # Allocate numpy array with shape (timestamps, instances)
        timestamps = len(result[0]["values"])
        instances = len(result)
        size = timestamps * instances

        # Format data
        data = np.fromiter(formatmatrix(result), dtype=np.uint64, count=size).reshape(timestamps, instances)
        return data

def formatforpreproc(rdata):
    """
    Format Prometheus query response into the expected format for preprocessors.
    Check out https://prometheus.io/docs/prometheus/latest/querying/api/ for the formatting information.
    
    params: Prometheus json response
    return:
    """
    rtype = rdata["data"]["resultType"]
    result = rdata["data"]["result"]

    # Process vector
    if rtype == "vector":
        # Allocate numpy array with shape (1, instances)
        # A vector only corresponds to one timestamp
        instances = len(result)
        size = instances

        # Format data
        data = np.fromiter(formatvector(result), dtype=np.uint64, count=size).reshape(1, size).T
        idx = np.arange(0, size).reshape(1, size).T
        return np.column_stack((data, idx))

    # Process matrix
    elif rtype == "matrix":
        # Allocate numpy array with shape (timestamps, instances)
        timestamps = len(result[0]["values"])
        instances = len(result)
        size = timestamps * instances

        # Format data
        data = np.fromiter(formatmatrix(result), dtype=np.uint64, count=size).reshape(timestamps, instances)
        return data


def formatvector(r):
    ts = r[0]["value"][0]
    for item in r:
        data = float(item["value"][1])
        yield data

def formatmatrix(r):
    for idx, val in enumerate(r[0]["values"]):
        ts = val[0]
        for inst in r:
            yield float(inst["values"][idx][1])

def formattimestamps(r, rtype):
    # Assume the same timestamps for all instances
    # Timestamps are only taken from the first instance
    if rtype == "vector":
        yield r[0]

    elif rtype == "matrix":
        for ts,_ in r:
            yield ts

if __name__=="__main__":
    url = "http://172.22.0.216:9090"

    start = "2022-06-26T00:00:00.000Z"
    end = "2022-06-29T19:30:00.000Z"

    client = PromClient(url)
    data = client.query_range("node_cpu_seconds_total{mode='user', host='prometheus-dev'}", start, end)
    #data = client.query("node_cpu_seconds_total{mode='user', host='prometheus-dev'} * 1000", start)
    #labels = client.label_val_meta(start, end, ["node_cpu_seconds_total{mode='user', host='prometheus-dev'}"], "cpu")

    print(data.nbytes)
    print(data)
