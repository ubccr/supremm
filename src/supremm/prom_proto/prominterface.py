import logging
import urllib.parse as urlparse

import numpy as np
import requests


HTTP_TIMEOUT = 5

class PromClient():

    def __init__(self, url):
        self._url = url

    def __str__(self):
        return self._url

    def query(self, query, time):
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
        plugin_data = formatforplugin(data)
        return plugin_data

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
        plugin_data = formatforplugin(data)
        return plugin_data

    def series_meta(self):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        params = {
            'match[]': matches,
            'start': start,
            'end': end
        }

        endpoint = "/api/v1/series"
        url = urlparse.urljoin(self._url, endpoint)
        return

    def label_val_meta(self, start, end, matches, label):
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
        # type(label) == set, len(label) == 1

        urlparse.urlencode(params, doseq=True)
        url = urlparse.urljoin(self._url, "/api/v1/label/%s/values" % label)
        logging.debug('Prometheus QUERY LABEL VALUES, url="(%s).20s" start=%s end=%s', url, start, end)

        # Query data
        r = requests.get(url, params=params, headers=headers)
        if r.status_code != 200:
            logging.error("Label Name Query Error: %s", r.content)
            return False
        data = r.json()
        names = data["data"]

        # Format for plugin
        label_idx = np.arange(0, len(names))
        return [label_idx, names]

def formatforplugin(rdata):
    """
    Format Prometheus query response into the expected format for SUPReMM plugins.
    Check out https://prometheus.io/docs/prometheus/latest/querying/api/ for the formatting information.
    
    params: Prometheus json response
    return: numpy array of dtype uint64
            matrices: [
                       [ts0 inst0 inst1 ... instN],
                       [ts1 inst0 inst1 ... instN],
                       ...
                       [tsN inst0 inst1 ... instN]
                      ]
            vectors: [[ts inst0 inst1 ... instN]]
    """
    rtype = rdata["data"]["resultType"]
    result = rdata["data"]["result"]

    # Process vector
    if rtype == "vector":
        # Allocate numpy array with shape (1, instances)
        # A vector only corresponds to one timestamp
        instances = len(result)
        size = instances

        # Format timestamps and data
        #ts = np.fromiter(formattimestamps(result[0]["value"], rtype), dtype=np.uint64, count=1)
        data = np.fromiter(formatvector(result), dtype=np.uint64, count=size).reshape(1, instances)
        return data #np.column_stack((ts, data))

    # Process matrix
    elif rtype == "matrix":
        # Allocate numpy array with shape (timestamps, instances)
        timestamps = len(result[0]["values"])
        instances = len(result)
        size = timestamps * instances

        # Format data
        #ts = np.fromiter(formattimestamps(result[0]["values"], rtype), dtype=np.uint64, count=timestamps)
        data = np.fromiter(formatmatrix(result), dtype=np.uint64, count=size).reshape(timestamps, instances)
        return data #np.column_stack((ts, data))

def formatvector(r):
    ts = r[0]["value"][0]
    for item in r:
        ### REMOVE * 1000 SCALING AFTER TESTING ###
        data = float(item["value"][1]) * 1000
        yield data

def formatmatrix(r):
    for idx, val in enumerate(r[0]["values"]):
        #ts = val[0]
        for inst in r:
            yield inst["values"][idx][1]

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

    start = "2022-07-01T00:30:00.000Z"
    end = "2022-07-03T09:00:00.000Z"

    client = PromClient(url)
    data = client.query_range("node_cpu_seconds_total{mode='user', host='prometheus-dev'} * 1000", start, end)
    data = client.query("node_cpu_seconds_total{mode='user', host='prometheus-dev'} * 1000", start)
    #labels = client.label_val_meta(start, end, ["node_cpu_seconds_total{mode='user', host='prometheus-dev'}"], "cpu")
    print(data)
