#!/usr/bin/env python
import sys
import json
import datetime
import os
import collections
import urllib
from optparse import OptionParser
from multiprocessing import Process
import re
from pymongo import MongoClient

PROCESS_ID = "pcpprocess.py 1.0.2"
verbose = True
job_metrics_enabled = False
overwrite_documents_in_db = True

mongo_host = "tas-db2"
mongo_port = 27017
mongo_db_name = "supremm"
mongo_db_collection_name = "mars"


def doinsert(db, records):
    db_collection = db[mongo_db_collection_name]
    try:
        db_collection.insert(records)
    except Exception as e:
        if overwrite_documents_in_db:
            insert_func = db_collection.save
        else:
            insert_func = db_collection.insert

        for r in records:
            try:
                insert_func(r)
            except Exception as e1:
                print e

def main():

    mclient = MongoClient(host=mongo_host)
    db = mclient[mongo_db_name]
    collection = db['mars']

    if len(sys.argv) < 2:
        print "Usage: {} [INPUTDIR]".format(os.path.basename(sys.argv[0]))

    inputdir = sys.argv[1]

    for root, dirs, files in os.walk(inputdir):
        for filename in files:
            if filename.endswith(".json"):
                with open(os.path.join(root, filename)) as fp:
                    j = json.load(fp)
                    collection.update({"_id": j["_id"]}, j, upsert=True)


if __name__ == "__main__":
    main()
