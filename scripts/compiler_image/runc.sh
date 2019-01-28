#!/bin/bash -e
eval "$(dx env --bash)"
docker run \
     --rm \
     -e DX_SECURITY_CONTEXT="$DX_SECURITY_CONTEXT" \
     -e DX_APISERVER_PROTOCOL="$DX_APISERVER_PROTOCOL" \
     -e DX_APISERVER_HOST="$DX_APISERVER_HOST" \
     -e DX_APISERVER_PORT=$DX_APISERVER_PORT \
     -e DX_PROJECT_CONTEXT_ID="$DX_PROJECT_CONTEXT_ID" \
     --volume "$PWD":/workdir \
     --workdir /workdir \
     dnanexus/dxwdl "$@"
