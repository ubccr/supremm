#!/bin/bash
set -euxo pipefail

BUILD=$1
case $BUILD in
  "rpm")
    python3 setup.py bdist_rpm
    ;;

  "wheel")
    python3 setup.py bdist_wheel
    ;;

  *)
    # EXIT
    ;;
esac
