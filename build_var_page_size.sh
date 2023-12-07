#!/bin/bash
set -e
set -x

# awk 'BEGIN{for(i=8;i<16;++i) for(l=8;l<16;++l) printf("-DPS_I=%d -DPS_L=%d\n",2^i,2^l)}' | ./build_var_page_size.sh

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

cd page-size-builds
renames="$(ls | sed 's:^.*$:"\0":;h; s:[ =-]:_:g;x; G; s:\n: :')"
echo "$renames" | xargs -L1 mv --