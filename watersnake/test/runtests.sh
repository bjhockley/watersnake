#! /bin/bash
# set -e
# set -x
cd "$(dirname "$0")"
export COVFILES=`realpath ../m*.py`
echo $COVFILES
PYTHONPATH=../ coverage run `which trial`  ./test_membership.py  && coverage report -m --include $COVFILES


