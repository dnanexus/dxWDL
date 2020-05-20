#!/bin/bash -ex

CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DXWDL_ROOT=$CURRENT_DIR/..
WDL_TOOLS_ROOT=$DXWDL_ROOT/../wdlTools

rm -rf $DXWDL_ROOT/src/main/scala/wdlTools
rm -rf $DXWDL_ROOT/src/main/java/Wdl*
mkdir $DXWDL_ROOT/src/main/scala/wdlTools

cp $WDL_TOOLS_ROOT/Makefile .
cp -r $WDL_TOOLS_ROOT/src/main/java/Wdl* $DXWDL_ROOT/src/main/java/
cp -r $WDL_TOOLS_ROOT/src/main/scala/wdlTools/{eval,syntax,types,util} $DXWDL_ROOT/src/main/scala/wdlTools
