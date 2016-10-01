#! /bin/bash
# set -e
# set -x
export DIV="------------------------------------------------------------------------------------------------"
cd "$(dirname "$0")"
PYTHONPATH=../../ python -m cProfile /usr/local/bin/trial ./test_membership.py
