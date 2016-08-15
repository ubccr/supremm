from __future__ import print_function
import sys
import os
from pymongo import MongoClient
from pymongo.errors import InvalidDocument
import json

class factory(object):
    """ output class generator helper """
    def __init__(self, config, resconf):
        outconf = config.getsection("outputdatabase")

        if outconf['db_engine'].lower() == "mongodb":
            self._impl = MongoOutput(outconf, resconf)
        elif outconf['db_engine'].lower() == "stdout":
            self._impl = StdoutOutput(outconf, resconf)
        elif outconf['db_engine'] == 'file':
            self._impl = FileOutput(outconf, resconf)
        else:
            raise Exception("Unsupported output mechanism {0}".format(outconf['db_engine']))

    def __enter__(self):
        return self._impl.__enter__()

    def __exit__(self, exception_type, exception_val, trace):
        return self._impl.__exit__(exception_type, exception_val, trace)


class FileOutput(object):
    """
    Simple outputter that dumps the job summary to a file given by out_file. Intended for debug purposes.
    """
    def __init__(self, outconf, resconf):
        self._resid = resconf['resource_id']
        self._path = outconf['out_file']
        if not os.path.exists(self._path):
            raise Exception("Path specified by out_file does not exist")
        self._file = open(self._path, 'w')
        self._dojson = True
        if self._dojson:
            self._jsonfile = open(self._path+'.json', 'w')
            self._jsonarray = []

    def __enter__(self):
        print ("Calling enter on file", file=self._file)
        return self

    def process(self, summary, mdata):
        """
        json print
        """ 
        print(self._resid, json.dumps(summary.get(), indent=4), file=self._file)
        print("MDATA: ", json.dumps(mdata, indent=4), file=self._file)
        if self._dojson:
            self._jsonarray.append(summary.get())
            self._jsonarray.append(mdata)

    def __exit__(self, exception_type, exception_val, trace):
        print("Calling exit on {}".format(self._file.name), file=self._file)
        self._file.close()
        if self._dojson:
            print(json.dumps(self._jsonarray, indent=4), file=self._jsonfile)
            self._jsonfile.close()

class MongoOutput(object):
    """ Support for mongodb output """
    def __init__(self, outconf, resconf):
        self._uri = outconf['uri']
        self._dname = outconf.get('dbname', outconf.get('db', 'supremm'))
        self._collection = "resource_" + str(resconf['resource_id'])
        self._timeseries = "timeseries-" + self._collection
        self._client = None
        self._outdb = None

    def __enter__(self):
        self._client = MongoClient(host=self._uri)
        self._outdb = self._client[self._dname]
        return self

    def process(self, summaryobj, mdata):
        """ output the summary record """

        summary = summaryobj.get()
        mongoid = str(summary['acct']['id']) + '-' + str(summary['acct']['end_time'])
        summary['_id'] = mongoid
        summary['summarization'].update(mdata)

        if 'timeseries' in summary:
            self._outdb[self._timeseries].update({"_id": summary["_id"]}, summary['timeseries'], upsert=True)
            del summary['timeseries']

        self._outdb[self._collection].update({"_id": summary["_id"]}, summary, upsert=True)

    def __exit__(self, exception_type, exception_val, trace):
        if self._client != None:
            self._outdb = None
            self._client.close()
            self._client = None

class StdoutOutput(FileOutput):
    """
    Simple outputter that dumps the job summary to stdout. Intended for debug purposes.
    """

    # Deliberately didn't call superclass constructor so it wouldn't throw exception to sys.stdout not being a filepath that exists
    def __init__(self, _, resconf):
        self._resid = resconf['resource_id']
        self._file = sys.stdout
        self._dojson = False

