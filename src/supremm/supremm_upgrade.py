#!/usr/bin/env python3
""" supremm-upgrade script used to alter database or config files to latest
    schema versions """

import argparse
import signal
import sys

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

    # Nothing to do for a 1.1 to 1.2 upgrade.

if __name__ == "__main__":
    main()
