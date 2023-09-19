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

while CONFIG= read -r line; do
  rm -f $targets
  env -S CONFIG make -j24 $targets
  mkdir -p page-size-builds/"$CONFIG"/
  mv $targets page-size-builds/"$CONFIG"/
done <"$input_file"
