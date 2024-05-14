#!/bin/bash
cd ..
DIRS="enbios/base enbios/bw2 demos enbios/generic"
python3 -m black --line-length 90 $DIRS
ruff check $DIRS --fix
