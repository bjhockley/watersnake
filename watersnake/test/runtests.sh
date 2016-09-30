#! /bin/bash
# set -e
# set -x
export DIV="------------------------------------------------------------------------------------------------"
cd "$(dirname "$0")"
PYTHONPATH=../ coverage run `which trial`  ./test_membership.py && echo "Coverage:" && echo $DIV && coverage report -m --include *watersnake* && echo $DIV && echo "Pylint code quality opinion: " && bash ./pylint.sh 2>&1 | grep "Your code has been rated"  && echo $DIV


