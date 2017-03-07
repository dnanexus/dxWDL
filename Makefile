SHELL=/bin/bash -e
GIT_VERSION := $(shell git describe --always --tags)

all:
	sbt "set test in assembly := {}" assembly
	mkdir -p applet_resources/resources
	(DX_WDL_JAR=$$(find target -name "dxWDL-*${GIT_VERSION}*.jar"); cp $$DX_WDL_JAR applet_resources/resources/dxWDL.jar)

test:
	sbt test

asset: remove_old_assets
	dx build_asset applet_resources

# This will remove existing dxWDL bundles from the project (only from the root folder)
remove_old_assets:
	(OLD_DXWDL_ASSETS=$$(dx find data --class record --name dxWDL --folder / --brief); if [ -n $$OLD_DXWDL_ASSETS ]; then for i in $$OLD_DXWDL_ASSETS; do dx rm $$i; done; fi)

clean:
	sbt clean clean-files
