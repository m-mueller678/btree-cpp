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

while read -r config; do
  rm -f $targets
  env PAGE_SIZE_OVERRIDE_FLAG="$config" make -j24 $targets
  mkdir -p page-size-builds/"$config"/
  mv $targets page-size-builds/"$config"/
done
