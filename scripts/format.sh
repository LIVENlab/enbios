#!/bin/bash
cd ..
DIRS="enbios/base enbios/bw2 enbios/demos enbios/ecoinvent enbios/generic enbios/models enbios/plotting enbios/test"
python3 -m black --line-length 90 $DIRS
ruff $DIRS --fix
