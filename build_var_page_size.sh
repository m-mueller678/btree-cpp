#!/bin/bash
set -e
set -x

rm page-size-builds -rf

targets=""

# Iterate through the array of files
for file in named-configs/*; do
  # Remove file extension
  filename=$(basename "$file")
  # Append the name to the variable
  targets="${targets} named-build/${filename%.*}-d0-ycsb named-build/${filename%.*}-n3-ycsb"
done

echo $targets

# Iterate through the arguments
for pageSize in "$@"; do
  rm -f $targets
  PAGE_SIZE_OVERRIDE_FLAG="-DPAGE_SIZE=$pageSize" make -j24 $targets
  mkdir -p page-size-builds/$pageSize/
  mv $targets page-size-builds/$pageSize/
done
