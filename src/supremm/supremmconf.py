#!/usr/bin/env python3
""" helper utiility to print out config info """
from __future__ import print_function

import sys
import os
import json
import logging
from getopt import getopt

from supremm.config import Config
from supremm.scripthelpers import setuplogger

def usage():
    """ print usage """
    print("usage: {0} [OPTS]".format(os.path.basename(__file__)))
    print("  -d --debug            set log level to debug")
    print("  -c --config           specify the path to the configuration file")
    print("  -s --section SECTION  output the configuration data from the specified section")
    print("  -i --item ITEM        output the configuration data for the specified item")
    print("  -h --help             print this help message")

def getoptions():
    """ process comandline options """

    retdata = {"log"	: logging.ERROR,
               "config"	: None,
               "section": None,
               "item"	: None}

    opts, _ = getopt(sys.argv[1:], "dc:s:i:h", ["debug", "config=", "section=", "item=", "help"])

    for opt in opts:
        if opt[0] in ("-d", "--debug"):
            retdata['log'] = logging.DEBUG 
        if opt[0] in ("-c", "--config"):
            retdata['config'] = opt[1]
        if opt[0] in ("-s", "--section"):
            retdata['section'] = opt[1]
        if opt[0] in ("-i", "--item"):
            retdata['item'] = opt[1]
        if opt[0] in ("-h", "--help"):
            usage()
            sys.exit(0)

    if 'section' in retdata:
        return retdata

    usage()
    sys.exit(1)

def main():
    """ print out config data according to cmdline args """
    opts = getoptions()

    setuplogger(opts['log'])

    if opts['config']:
        logging.debug("Using specified path: {}".format(opts['config']))
    else:
        logging.debug("Automatically detecting configuration path.")

    try:
        conf = Config(opts['config'])
    except:
        logging.error("Configuration could not be found.")
        sys.exit(1)

    if not opts['section']:
       print(conf)
       sys.exit(0)

    try:
        section = conf.getsection(opts['section'])
    except KeyError:
        logging.error("Section '{}' not defined in configuration file.".format(opts['section']))
        sys.exit(1)

    if opts['item']:
        try:
            item = section[opts['item']]
        except KeyError:
            logging.error("Item '{}' not defined in section '{}'.".format(opts['item'], opts['section']))
            sys.exit(1)

        if isinstance(item, dict):
            item = json.dumps(item, indent=4)
 
        print(item)

    else:
        print(json.dumps(section, indent=4))

if __name__ == "__main__":
    main()
