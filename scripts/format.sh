#!/bin/bash
cd ..
DIRS="enbios"
python3 -m black --line-length 90 $DIRS
ruff check $DIRS --fix
