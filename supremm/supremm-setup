#!/usr/bin/env python
import json
import os
import tempfile
import copy
from supremm.scripthelpers import getdbconnection
from supremm.config import Config
from supremm.xdmodstylesetupmenu import XDMoDStyleSetupMenu
import ConfigParser
import socket
import subprocess
import sys
import signal

def getsharedir():
    """ return the path to the share/ directory """
    return os.path.realpath(os.path.join(os.path.dirname(__file__), "../share/supremm"))

def promptconfig(display):
    """ prompt user for configuration path """
    config = None
    while config == None:
        confpath = display.prompt_string("Enter path to configuration files", Config.autodetectconfpath())
        try:
            config = Config(confpath)
            config.getsection('datawarehouse')
            config.getsection('outputdatabase')
        except Exception as e:
            errmsg = """
Error unable to read valid configuration file in directory
{0}
Please enter a valid config path (or ctrl-c to exit this program)
"""
            display.newpage()
            display.print_text(errmsg.format(confpath))
            config = None

    return config

def getvalidpath(display, prompt, defaultpath):
    """ repeatedly pester user for a valid configuration path """
    while True:
        outpath = display.prompt_string(prompt, defaultpath)
        try:
            filep = open(os.path.join(outpath, "config.json"), "a")
            filep.close()
            return outpath

        except IOError:
            errmsg = """
Error unable to write to configuration files in directory
{0}
Please enter a path to a writable directory (or ctrl-c to exit this program)
"""
            display.newpage()
            display.print_text(errmsg.format(outpath))

def getxdmodsettings(display, defaults):
    """ obtain the database settings from XDMoD """
    display.print_text("DB setup based on XDMoD path specification")
    xdmodpath = display.prompt_string("  XDMoD configuration directory path", defaults['xdmodpath'])

    outconfig = {"xdmodroot": xdmodpath,
                 "datawarehouse": {"include": "xdmod://datawarehouse"},
                 "outputdatabase": {"include": "xdmod://jobsummarydb"}}

    return outconfig

def getdirectsettings(display, defaults):
    """ obtain the database settings from user """
    display.print_text("Direct DB Credentials")
    sqlhost = display.prompt_string("  XDMoD mysqldb hostname", defaults['mysqlhostname'])
    sqlport = display.prompt_string("  XDMoD mysqldb port number", defaults['mysqlport'])
    sqluser = display.prompt_string("  XDMoD mysqldb username", defaults['mysqlusername'])
    sqlpass = display.prompt_password("  XDMoD mysqldb password")
    sprompt = "  Location of my.cnf file (where the username and passsword will be stored)"
    mycnffilename = display.prompt_string(sprompt, defaults['mycnffilename'])
    mongouri = display.prompt_string("  MongoDB URI", defaults['mongouri'])
    mongodbname = display.prompt_string("  MongoDB database name", defaults['mongodb'])

    outconfig = {"datawarehouse": {"db_engine": "MySQLDB",
                                   "host": sqlhost,
                                   "port": int(sqlport),
                                   "defaultsfile": mycnffilename},
                 "outputdatabase": {"db_engine": "mongodb",
                                    "uri": mongouri,
                                    "dbname": mongodbname}}
    mycnf = "[client]\nuser={0}\npassword={1}\n".format(sqluser, sqlpass)

    return outconfig, mycnf

def default_settings(confpath):
    """ populate the default settings for the configuration.
        will use the values specifed in the configuration file if it exists.
        """
    defaults = {}

    defaults['usexdmodconfig'] = 'y'
    defaults['xdmodpath'] = '/etc/xdmod'
    defaults['archiveoutdir'] = '/dev/shm/supremm'
    defaults['mysqlhostname'] = 'localhost'
    defaults['mysqlport'] = 3306
    defaults['mysqlusername'] = 'xdmod'
    defaults['mycnffilename'] = '~/.supremm.my.cnf'
    defaults['mongouri'] = 'mongodb://localhost:27017/supremm'
    defaults['mongodb'] = 'supremm'

    try:
        existingconf = Config(confpath)
        rawconfig = existingconf._config.copy()

        if 'xdmodroot' in rawconfig and 'datawarehouse' in rawconfig and 'include' in rawconfig['datawarehouse']:
            defaults['usexdmodconfig'] = 'y'
            defaults['xdmodpath'] = rawconfig['xdmodroot']
        else:
            dwconfig = existingconf.getsection('datawarehouse')
            defaults['usexdmodconfig'] = 'n'
            defaults['mysqlhostname'] = dwconfig['host']
            defaults['mysqlport'] = dwconfig.get('port', defaults['mysqlport'])
            defaults['mycnffilename'] = dwconfig['defaultsfile']

            try:
                mycnf = ConfigParser.RawConfigParser()
                mycnf.read(os.path.expanduser(dwconfig['defaultsfile']))
                if mycnf.has_section('client'):
                    defaults['mysqlusername'] = mycnf.get('client', 'user')
            except ConfigParser.Error:
                pass

            outputconfig = existingconf.getsection('outputdatabase')
            defaults['mongouri'] = outputconfig['uri']
            defaults['mongodb'] = outputconfig['dbname']

        summarycnf = existingconf.getsection('summary')
        defaults['archiveoutdir'] = summarycnf['archive_out_dir']

        defaults['resources'] = rawconfig['resources']

    except Exception as e:
        # ignore missing or broken existing config files.
        pass

    return defaults

def create_config(display):
    """ Create the configuration file """
    display.newpage("Configuration File setup (DB)")

    confpath = getvalidpath(display,
                            "Enter path to configuration files",
                            Config.autodetectconfpath())

    defaults = default_settings(confpath)

    display.newpage()
    display.print_text("""XDMoD datawarehouse access credentials.

There are two options to specify the XDMoD datawarehouse access credentials.
Either specify the path to the XDMoD install or specify the hostname, username,
password of the database directly.
""")

    doxdmod = display.prompt("Do you wish to specify the XDMoD install directory", ["y", "n"], defaults['usexdmodconfig'])

    mycnf = None

    outconfig = {}

    if doxdmod == "y":
        outconfig = getxdmodsettings(display, defaults)
    else:
        outconfig, mycnf = getdirectsettings(display, defaults)

    archivedir = display.prompt_string("  Temporary directory to use for job archive processing", defaults['archiveoutdir'])

    outconfig["summary"] = {"archive_out_dir": archivedir,
                            "subdir_out_format": "%r/%j"}

    display.newpage("Configuration File setup (Resources)")
    display.print_text("Autodetecting resources based on configuration file settings")

    outconfig["resources"] = {}
    try:
        config = generatetempconfig(outconfig, mycnf)
        dbconn = getdbconnection(config.getsection("datawarehouse"))
        dbcur = dbconn.cursor()
        dbcur.execute("SELECT id as resource_id, code as resource FROM modw.resourcefact")
        for resource in dbcur:
            resconf = configure_resource(display, resource[0], resource[1], defaults)
            outconfig['resources'][resource[1]] = resconf

        writeconfig(display, confpath, outconfig, mycnf)
    except Exception as exc:
        display.print_warning("An error occurred while detecting resources.\n{0}".format(exc))
        display.hitanykey("Press ENTER to continue.")

def promptwritefile(display, filepath, contents):
    """ double check that its ok to overwrite config file and provide
        feedback if it didn't work """
    dowrite = display.prompt("Overwrite config file '{0}'".format(filepath), ['y', 'n'], 'y')
    if dowrite == 'y':
        try:
            display.print_text("Writing configuration to '{0}'".format(filepath))
            with open(filepath, "w") as filep:
                filep.write(contents)
        except IOError:
            display.print_warning("Failed to write config file.")
    else:
        display.print_warning("Changes NOT saved!")

    display.hitanykey("Press ENTER to continue.")

def writeconfig(display, confpath, outconfig, mycnf):
    """ write config settings and prompt user """

    display.newpage("Write config files")

    promptwritefile(display, os.path.join(confpath, "config.json"), json.dumps(outconfig, indent=4))

    if mycnf != None:
        mycnfpath = os.path.expanduser(outconfig['datawarehouse']['defaultsfile'])
        promptwritefile(display, mycnfpath, mycnf)

def generatetempconfig(confdata, mycnf):
    """ Generate a configuration object based on the config settings """
    confpath = tempfile.mkdtemp()

    tmpconfdata = copy.deepcopy(confdata)

    if mycnf != None:
        mycnfpath = os.path.join(confpath, "my.cnf")
        tmpconfdata['datawarehouse']['defaultsfile'] = mycnfpath
        with open(mycnfpath, "w") as tmpmycnf:
            tmpmycnf.write(mycnf)

    with open(os.path.join(confpath, "config.json"), "w") as tmpconfig:
        json.dump(tmpconfdata, tmpconfig, indent=4)

    config = Config(confpath)

    return config

def get_hostname_ext():
    """ get the domain name part for the current host """
    fqdn = socket.getfqdn()
    tokens = fqdn.split(".")
    if len(tokens) > 1:
        return ".".join(tokens[1:])
    else:
        return ""

def configure_resource(display, resource_id, resource, defaults):
    """ get the configuration settings for a resource """

    display.newpage("Configuration File setup (Resources)")
    display.print_text("Configuration for " + resource)

    setting = {"resource_id": resource_id,
               "enabled": True,
               "batch_system": "XDMoD",
               "hostname_mode": "hostname",
               "host_name_ext": get_hostname_ext(),
               "pcp_log_dir": "/data/" + resource + "/pcp-logs",
               "script_dir": "/data/" + resource + "/jobscripts"}

    descriptions = {"resource_id": None,
                    "enabled": "Enable SUPReMM summarization for this resource?",
                    "batch_system": "Source of accounting data",
                    "hostname_mode": "node name unique identifier ('hostname' or 'fqdn')",
                    "host_name_ext": "domain name for resource",
                    "pcp_log_dir": "Directory containing node-level PCP archives",
                    "script_dir": "Directory containing job launch scripts (enter [space] for none)"}

    keys = ["enabled", "pcp_log_dir", "batch_system", "hostname_mode", "host_name_ext", "script_dir"]

    resdefault = {}

    try:
        if defaults['resources'][resource]['resource_id'] == resource_id:
            resdefault = defaults['resources'][resource]
    except KeyError:
        pass

    for key in keys:
        if key == "host_name_ext" and setting["hostname_mode"] == "hostname":
            del setting["host_name_ext"]
            continue

        setting[key] = display.prompt_input(descriptions[key], resdefault.get(key, setting[key]))

        if key == "enabled" and setting[key] == False:
            break

        if key == 'pcp_log_dir':
            if not os.path.isdir(setting[key]):
                display.print_warning("""
WARNING The directory {0} does not exist. Make sure to create and populate this
directory before running the summarization software.
""".format(setting[key]))

    return setting

def create_mysql_tables(display):
    """ Create the tables in the datawarehouse """

    display.newpage("MySQL Database setup")

    config = promptconfig(display)

    scriptpath = os.path.join(getsharedir(), "setup/modw_supremm.sql")

    dbsettings = config.getsection("datawarehouse")
    host = display.prompt_string("DB hostname", dbsettings['host'])
    port = display.prompt_string("DB port", dbsettings['port'] if 'port' in dbsettings else 3306)
    myrootuser = display.prompt_string("DB Admin Username", "root")
    myrootpass = display.prompt_password("DB Admin Password")

    display.print_warning("""

WARNING This operation will delete the contents of existing tables

""")

    dotables = display.prompt("Do you wish to proceed?", ["y", "n"], "n")

    if dotables == "y":
        pflag = "-p{0}".format(myrootpass) if myrootpass != "" else ""
        shellcmd = "mysql -u {0} {1} -h {2} -P {3} < {4}".format(myrootuser,
                                                                 pflag,
                                                                 host,
                                                                 port,
                                                                 scriptpath)
        try:
            retval = subprocess.call(shellcmd, shell=True)
            if retval != 0:
                display.print_warning("""

An error occurred creating the tables. Please create the tables manually
following the documentation in the install guide.
""")
            else:
                display.print_text("Sucessfully created tables")
        except OSError as e:
            display.print_warning("""

An error:

\"{0}\" 

occurred running the mysql command. Please create the tables manually
following the documentation in the install guide.
""".format(e.strerror)) 

        display.hitanykey("Press ENTER to continue.")

def create_mongodb(display):
    """ Add the schema collection to mongo """

    display.newpage("Mongo Database setup")

    config = promptconfig(display)

    scriptpath = os.path.join(getsharedir(), "setup/mongo_setup.js")

    dbsettings = config.getsection("outputdatabase")

    mongouri = display.prompt_string("URI", dbsettings['uri'])

    if mongouri.startswith("mongodb://"):
        mongouri = mongouri[10:]

    display.print_warning("""

WARNING This operation will write to mongo

""")

    dotables = display.prompt("Do you wish to proceed?", ["y", "n"], "n")

    if dotables == "y":
        command = ["mongo", "--quiet", mongouri, scriptpath]
        try:
            retval = subprocess.call(command)
            if retval != 0:
                display.print_warning("""

An error occurred writing to mongo. Please refer to the manual setup
instructions in the install guide.
""")
            else:
                display.print_text("Sucessfully updated mongo")
        except OSError as e:
            display.print_warning("""

An error:

\"{0}\" 

occurred running the mongo command. Please refer to the manual setup
instructions in the install guide.

""".format(e.strerror)) 

    display.hitanykey("Press ENTER to continue.")

def signal_handler(sig, _):
    """ clean exit on an INT signal """
    if sig == signal.SIGINT:
        sys.exit(0)

def main():
    """ main """

    items = [("c", "Create configuration file", create_config),
             ("d", "Create MySQL database tables", create_mysql_tables),
             ("m", "Initalize MongoDB database", create_mongodb),
             ("q", "Quit", None)]

    signal.signal(signal.SIGINT, signal_handler)

    with XDMoDStyleSetupMenu() as display:
        display.show_menu("SUPReMM Job Summarization package setup", items)

if __name__ == "__main__":
    main()
