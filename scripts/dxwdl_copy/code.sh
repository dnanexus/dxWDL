#!/bin/bash

# The following line causes bash to exit at any point if there is any error
# and to output each line as it is executed -- useful for debugging
set -e -x -o pipefail

main() {
    if [[ $DX_JOB_ID == "aws:cn-north-1" ]]; then
        url=$(echo $url | sed -e "s/dl\.dnanex\.us/dl\.cn\.dnanexus.com:8002/g")
    fi

    fid=$(wget -qO- ${url} | dx upload - --brief --visibility hidden --destination ${DX_PROJECT_CONTEXT_ID}:${folder}/${filename})

    dx-jobutil-add-output ofile --class=file $fid
}
