#!/bin/bash
set -e -x -o pipefail

main() {
    
    f1="out"
	f2="out_2"
    dx mkdir "$DX_PROJECT_CONTEXT_ID:/${f1}"
	dx mkdir "$DX_PROJECT_CONTEXT_ID:/${f2}"

    if [ "${config_name}" != "" ]; then
	
		dx download "$config" -o config
    
	fi
	
	outputs=()
	for i in ${!inputs[@]}; do

        dx download "${inputs[$i]}" -o inputs-$i
		if [ $(($i % 2)) == 0 ]; then
        
			fid=$(dx upload inputs-$i --path "$DX_PROJECT_CONTEXT_ID:/$f1/" --brief)

        else
	
			fid=$(dx upload inputs-$i --path "$DX_PROJECT_CONTEXT_ID:/$f2/" --brief)

		fi

		dx-jobutil-add-output outputs "${fid}" --class=array:file

    done

}
