#!/bin/bash
set -e

INSTALL_TYPE=$1
bootstrap.sh $INSTALL_TYPE

integration_test.bash

## Run component tests
