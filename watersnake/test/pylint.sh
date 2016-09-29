#! /bin/bash
# set -e
# set -x
cd "$(dirname "$0")"
PYTHONPATH=.. pylint -i y ./test_membership.py ../membership.py

