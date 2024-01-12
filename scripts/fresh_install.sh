#!/bin/bash

cd ..

cd dist || exit

# Sort files alphabetically and get the file with the highest version
latest_file=$(ls -v | grep 'enbios-[0-9].' | tail -n 2)

# Get the tar.gz of that file
latest_tar_gz=$(ls -v | grep "$latest_file.*\.tar\.gz$" | tail -n 1)

# Get the absolute path of the latest tar.gz file
absolute_path=$(realpath "$latest_tar_gz")

cd ..

# Store current directory in project_dir
project_dir=$(pwd)

cd ..

if [ -d "enbios_install" ]; then
  rm -r "enbios_install"
  echo "removed existing enbios_install directory"
fi

mkdir enbios_install
cd enbios_install || exit
#cp $absolute_path .
echo "creating venv"
python3 -m venv venv
. venv/bin/activate
echo "installing enbios"
pip install "$absolute_path"

# Copy the file from project_dir/enbios/demo/a.py to this directory
cp "$project_dir"/enbios/demos/demo_script.py .
python3 demo_script.py