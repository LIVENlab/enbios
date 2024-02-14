#!/bin/bash
cd ..
DIRS="enbios/base enbios/bw2 enbios/demos enbios/generic enbios/models"
python3 -m black --line-length 90 $DIRS
ruff $DIRS --fix
