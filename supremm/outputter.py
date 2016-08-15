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
        else:
            raise Exception("Unsupported output mechanism {0}".format(outconf['db_engine']))

    def __enter__(self):
        return self._impl.__enter__()

    def __exit__(self, exception_type, exception_val, trace):
        return self._impl.__exit__(exception_type, exception_val, trace)


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


class StdoutOutput(object):
    """
    Simple outputter that dumps the job summary to stdout. Intended for debug purposes.
    """
    def __init__(self, _, resconf):
        self._resid = resconf['resource_id']

    def __enter__(self):
        print "Calling enter on stdout"
        return self

    def process(self, summary, mdata):
        """
        json print
        """
        print self._resid, json.dumps(summary.get(), indent=4)
        print "MDATA: ", json.dumps(mdata, indent=4)

    def __exit__(self, exception_type, exception_val, trace):
        print "Calling exit on stdout"
