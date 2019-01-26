#!/bin/bash -e
eval "$(dx env --bash)"
sudo docker run --rm -e DX_SECURITY_CONTEXT="$DX_SECURITY_CONTEXT" -v $pwd:/tmp/workdir dnanexus/dxwdl "$@"
