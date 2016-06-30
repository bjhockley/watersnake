#! /bin/bash
cd "$(dirname "$0")"
PYTHONPATH=../ trial ./test_membership.py
