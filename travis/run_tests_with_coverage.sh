#!/bin/bash

set -e;
for run_file in tests/run_*.py ;
do
  echo "$run_file";
  python -m coverage run --append "$run_file";
done ;
