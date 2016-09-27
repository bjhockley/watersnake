#! /bin/bash
cd "$(dirname "$0")"
# PYTHONPATH=../ trial test_membership.TestWaterSnake.test_statefulness
PYTHONPATH=../ trial ./test_membership.py
