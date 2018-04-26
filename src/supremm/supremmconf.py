#!/usr/bin/env python
""" helper utiility to print out config info """

import sys
import os
import json
from getopt import getopt

from supremm.config import Config

def usage():
    """ print usage """
    print "usage: {0} [OPTS]".format(os.path.basename(__file__))
    print "  -s --section SECTION  output the configuration data from the specified section"
    print "  -i --item ITEM        output the configuration data for the specified item"
    print "  -h --help             print this help message"


def getoptions():
    """ process comandline options """

    retdata = {"section": None, "item": None}

    opts, _ = getopt(sys.argv[1:], "s:i:h", ["section=", "item="])

    for opt in opts:
        if opt[0] in ("-s", "--section"):
            retdata['section'] = opt[1].encode("utf-8")
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
    conf = Config()
    
    try:
        section = conf.getsection(opts['section'])
        if opts['item'] != None:
            print section[opts['item']]
        else:
            print json.dumps(section, indent=4)

    except KeyError:
        sys.stderr.write("Error section \"%s\" not defined in configuration file.\n" % (opts['section']))
        sys.exit(1)

if __name__ == "__main__":
    main()
