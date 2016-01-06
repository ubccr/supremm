#!/usr/bin/env python
""" Configuration data management """
import os
import json
import ConfigParser
import re

def iscomment(line):
    """ check is line is a c++ style comment """
    if re.search(r"^\s*//", line):
        return True
    return False

class Config(object):
    """ Configuration data management
        The configuration file format is similar to json except lines that begin "//"
        are treated as comments and are ignored. Also the string \n[:space:]// is not permitted
        anywhere in json key or value.
    """

    def __init__(self, confpath=None):

        if confpath == None:
            # Try to guess the location of the config directory
            searchpaths = [os.path.dirname(os.path.abspath(__file__)) + "/../../../../etc/supremm",
                           "/etc/supremm"]
            confpath = self.findpath(searchpaths, "config.json")

        if os.path.isdir(confpath) == False:
            raise Exception("Missing configuration path %s" % confpath)

        conffile = os.path.join(confpath, "config.json")
        with open(conffile, "rb") as conffp:
            confdata = ""
            for line in conffp:
                if not iscomment(line):
                    confdata += line
            try:
                self._config = json.loads(confdata)
            except ValueError as exc:
                raise Exception("Syntax error in %s.\n%s" % (conffile, str(exc)))

        self._xdmodconfig = None

    @staticmethod
    def findpath(pathlist, fname):
        for path in pathlist:
            if os.path.exists(os.path.join(path, fname)):
                return os.path.abspath(path)
        return None

    def getsection(self, sectionname):
        """ return the dict for a given section """

        if "include" in self._config[sectionname]:
            self._config[sectionname] = self.process_include(sectionname, self._config[sectionname]['include'])

        return self._config[sectionname]

    def parsexdmod(self):
        """ locate and parse the XDMoD portal settings file """
        self._xdmodconfig = ConfigParser.RawConfigParser()
        xdmodconfs = [os.path.join(self._config['xdmodroot'], "portal_settings.ini"),
                      os.path.join(self._config['xdmodroot'], "etc/portal_settings.ini"),
                      os.path.join(self._config['xdmodroot'], "configuration/portal_settings.ini")]

        nread = self._xdmodconfig.read(xdmodconfs)
        if len(nread) == 0:
            raise Exception("Unable to read XDMoD configuration file. Locations scanned: %s", xdmodconfs)

    @staticmethod
    def strtonative(value):
        v = value.strip("\"")
        try:
            return int(v)
        except ValueError:
            return v

    def process_include(self, sectionname, url):
        """ process an include directive (only xdmod parsing is supported) """
        if url.startswith("xdmod://"):
            if self._xdmodconfig == None:
                self.parsexdmod()

            xdmodsection = url[8:]
            if not self._xdmodconfig.has_section(xdmodsection):
                raise Exception("Unable to locate include data for %s", url)

            result = {}
            for k, v in self._xdmodconfig.items(xdmodsection):
                result[k] = self.strtonative(v)

            return result
        else:
            raise Exception("Unsupported include url %s in section %s", url, sectionname)

    def resourceconfigs(self):
        """ Iterator over enabled resources """
        for resname, resdata in self._config['resources'].iteritems():
            if "enabled" in resdata and resdata['enabled'] == False:
                continue
            resdata['name'] = resname
            yield (resname, resdata)

def test():
    """ test """
    conf = Config()
    print conf.getsection("datawarehouse")
    # for r, d in c.resourceconfigs():
    #    print r, d

if __name__ == "__main__":
    test()
