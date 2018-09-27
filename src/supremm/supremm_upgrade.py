#!/usr/bin/env python
""" supremm-upgrade script used to alter database or config files to latest
    schema versions """

import argparse
import signal
import subprocess
import sys

import pkg_resources

from MySQLdb import ProgrammingError

from supremm.scripthelpers import getdbconnection
from supremm.config import Config
from supremm.xdmodstylesetupmenu import XDMoDStyleSetupMenu


def checkForPreviousInstall(display, dbsettings):
    """ Query the database to check that the database table from a 1.0 install is present """

    dbcon = getdbconnection(dbsettings)
    try:
        cur = dbcon.cursor()
        cur.execute('SELECT 1 FROM `modw_supremm`.`archive` LIMIT 1')
        cur.close()
    except ProgrammingError:
        display.print_warning("""No previous install detected. No migration will be performed. Please refer to
the documentation for instructions on how to setup a new instance of the
software using the 'supremm-setup' command.
""")
        display.hitanykey("Press ENTER to continue.")
        sys.exit()

    dbcon.close()

def updateMysqlTables(display, opts):
    """ Interactive mysql script execution """

    config = Config()
    dbsettings = config.getsection("datawarehouse")

    checkForPreviousInstall(display, dbsettings)

    migration = pkg_resources.resource_filename(__name__, "migrations/1.0-1.1/modw_supremm.sql")

    host = dbsettings['host']
    port = dbsettings['port'] if 'port' in dbsettings else 3306

    display.newpage("MySQL Database setup")
    myrootuser = display.prompt_string("DB Admin Username", "root")
    myrootpass = display.prompt_password("DB Admin Password")

    pflag = "-p{0}".format(myrootpass) if myrootpass != "" else ""
    shellcmd = "mysql -u {0} {1} -h {2} -P {3} < {4}".format(myrootuser,
                                                             pflag,
                                                             host,
                                                             port,
                                                             migration)
    try:
        if opts.debug:
            display.print_text(shellcmd)

        retval = subprocess.call(shellcmd, shell=True)
        if retval != 0:
            display.print_warning("""

An error occurred migrating the tables. Please create the tables manually
following the documentation in the install guide.
""")
        else:
            display.print_text("Sucessfully migrated tables")
    except OSError as e:
        display.print_warning("""

An error:

\"{0}\"

occurred running the mysql command. Please create the tables manually
following the documentation in the install guide.
""".format(e.strerror))

    display.hitanykey("Press ENTER to continue.")


def signalHandler(sig, _):
    """ clean exit on an INT signal """
    if sig == signal.SIGINT:
        sys.exit(0)


def main():
    """ main entry point """
    parser = argparse.ArgumentParser(description='Upgrade the SUPReMM database and config files')
    parser.add_argument('-v', '--verbose', action='store_true', help='Output info level logging')
    parser.add_argument('-d', '--debug', action='store_true', help='Output debug level logging')
    parser.add_argument('-q', '--quiet', action='store_true', help='Output warning level logging')

    opts = parser.parse_args()

    signal.signal(signal.SIGINT, signalHandler)

    with XDMoDStyleSetupMenu() as display:
        updateMysqlTables(display, opts)


if __name__ == "__main__":
    main()
