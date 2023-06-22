#!/bin/bash
set -euxo pipefail

declare -a builds=("rpm" "wheel" "src")
for BUILD in "${builds[@]}";
do
case $BUILD in
  "rpm")
    python3 setup.py bdist_rpm
    ;;

  "wheel")
    python3 setup.py bdist_wheel
    ;;

  "src")
    tar --exclude={'*.rpm','*.whl'} -czf /tmp/supremm.tar.gz .
    mv /tmp/supremm.tar.gz dist
    ;;

  *)
    ;;
esac
done
