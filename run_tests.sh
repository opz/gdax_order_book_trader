#!/bin/bash

DIR="$(dirname "$(readlink -f "$0")")"
python -m unittest discover $DIR "test_*.py"
