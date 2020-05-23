#!/bin/bash -e

files=$(find . -name "*.scala")

for f in $files; do
    echo $f
    sed -f dewom.sed $f > $f.2
    mv -f $f.2 $f
done
