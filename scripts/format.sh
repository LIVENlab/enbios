#!/bin/bash
cd ..
DIRS="enbios2/base enbios2/bw2 enbios2/demos enbios2/ecoinvent enbios2/generic enbios2/models enbios2/plotting enbios2/test"
python3 -m black --line-length 90 $DIRS
ruff $DIRS --fix
