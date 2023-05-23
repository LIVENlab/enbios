#!/bin/bash

sphinx-apidoc -f -o source/enbios2  enbios2
sphinx-build -b html source build
