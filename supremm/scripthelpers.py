#!/usr/bin env python
""" common functions used in the command line scripts """

import re
import datetime
import MySQLdb
import MySQLdb.cursors

def parsetime(strtime):
    """ Try to be flexible in the time formats supported:
           1) unixtimestamp prefixed with @
           2) year-month-day zero-padded
           3) year-month-day hour:minute:second zero padded optional T between date and time
           4) locale specific format
    """
    m = re.search(r"^@(\d*)$", strtime)
    if m:
        return datetime.datetime.fromtimestamp(int(m.group(1)))
    if re.search(r"^\d{4}-\d{2}-\d{2}$", strtime):
        return datetime.datetime.strptime(strtime, "%Y-%m-%d")
    m = re.search(r"^(\d{4}-\d{2}-\d{2}).(\d{2}:\d{2}:\d{2})$", strtime)
    if m:
        return datetime.datetime.strptime(m.group(1) + " " + m.group(2), "%Y-%m-%d %H:%M:%S")

    return datetime.datetime.strptime(strtime, "%c")

def getdbconnection(configsection, as_dict=False):
    """ Helper function that gets a database connection object from a config dictionary """

    dbengine = configsection['dbengine'] if 'dbengine' in configsection else 'MySQLDB'

    if dbengine == 'MySQLDB':

        # Convert the external configuration names to python PEP-249 config names
        translate = {"host": "host", 
                     "defaultsfile": "read_default_file",
                     "user": "user",
                     "pass": "passwd",
                     "port": "port"}

        dbargs = {}
        for confval, myval in translate.iteritems():
            if confval in configsection:
                dbargs[myval] = configsection[confval]

        if as_dict:
            dbargs['cursorclass'] = MySQLdb.cursors.DictCursor

        return MySQLdb.connect(**dbargs)
    else:
        raise Exception("Unsupported database engine %s" % (dbengine))

