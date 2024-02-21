#!/bin/bash
no_update_present=false

for arg in "$@"
do
  if [ "$arg" == "--no-update" ]; then
    no_update_present=true
  fi
done
echo "Is --no-update present: $no_update_present"


#echo "editing pyproject.toml hacking in requirements.txt..."
#cd ..
#. venv_build/bin/activate
#cd scripts || exit 1
#python3 include_dependencies.py
#echo "dependencies included"

if [ "$no_update_present" = false ] ; then
  output=$(python3 version_inc.py)
  echo "new version: ..."
  echo $output

fi
cd ..

python3 -m build
# status of last command
if [ $? -eq 1 ]; then
  echo "built failed. reverting version"
  cd scripts || exit 1
  if [ "$no_update_present" = false ] ; then
    python3 version_inc.py revert
  fi
  exit 1
fi

shopt -s nullglob

for file in dist/*"$output"*; do
  echo "$file"
done

python3 -m twine upload --config-file .pypirc --repository pypi dist/*"$output"*

if [ $? -eq 1 ]; then
  echo "upload failed. reverting version"
  cd scripts || exit 1
  python3 version_inc.py revert
  exit 1
fi
