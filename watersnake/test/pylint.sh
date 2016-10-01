#! /bin/bash
# set -e
# set -x
cd "$(dirname "$0")"
export COVFILES=`find ../  | egrep "\.py$" | xargs realpath | tr "\n" " "`
PYTHONPATH=.. pylint --output-format=parseable --reports=y $COVFILES
