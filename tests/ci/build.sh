#!/bin/bash
set -euxo pipefail

build=("rpm" "wheel" "src")
for $BUILD in "${build[@]}"
do
case
  "rpm")
    python3 setup.py bdist_rpm
    ;;

  "wheel")
    python3 setup.py bdist_wheel
    ;;

  "src")
    tar -czf supremm.tar.gz -C dist
    ;;

  *)
    ;;
esac
done
