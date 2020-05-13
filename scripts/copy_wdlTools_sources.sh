#!/bin/bash -e

CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DXWDL_ROOT=$CURRENT_DIR/..
WDL_TOOLS_ROOT=$DXWDL_ROOT/../wdlTools

rm -rf $DXWDL_ROOT/src/main/scala/wdlTools
rm -rf $DXWDL_ROOT/src/main/antlr4

cp $WDL_TOOLS_ROOT/Makefile .
cp -r $WDL_TOOLS_ROOT/src/main/antlr4 $DXWDL_ROOT/src/main/
cp -r $WDL_TOOLS_ROOT/src/main/scala/wdlTools $DXWDL_ROOT/src/main/scala
