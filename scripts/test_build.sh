#!/bin/bash
. build.sh --no-update
cd scripts || exit
. fresh_install.sh
. fresh_install_py12.sh