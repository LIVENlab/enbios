#!/bin/bash
#. build.sh --no-update
#cd scripts || exit
. fresh_install.sh
# go back to orig directory
cd "$project_dir/scripts" || exit
. fresh_install_py12.sh