SHELL=/bin/bash -e

all:
	${SBT_PATH} assembly
	mkdir -p applet_resources/resources
	(DX_WDL_JAR=$$(find target -name "dxWDL.jar"); cp $$DX_WDL_JAR applet_resources/resources/dxWDL.jar)

test:
	${SBT_PATH} test

asset:
	dx build_asset applet_resources

clean:
	${SBT_PATH} clean clean-files
