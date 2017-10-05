#!/bin/bash

# The following line causes bash to exit at any point if there is any error
# and to output each line as it is executed -- useful for debugging
set -e -x -o pipefail

main() {
    diff $a $b > "X.txt" || true

    equivalent="true"
    if [[ -s X.txt ]]; then
        equivalent="false"
    fi

    dx-jobutil-add-output --class=boolean equality $equivalent
}
