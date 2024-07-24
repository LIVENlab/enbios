#!/bin/bash
#!/bin/bash
no_update_present=false
next_version_beta=false

for arg in "$@"
do
  if [ "$arg" == "--no-update" ]; then
    no_update_present=true
  fi

  if [ "$arg" == "--next-version-beta" ]; then
    next_version_beta=true
  fi
done
echo "Is --no-update present: $no_update_present"
echo "Is --next-version-beta: $next_version_beta"

if [ "$next_version_beta" = true ] ; then
  output=$(python3 version_inc.py beta)
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
