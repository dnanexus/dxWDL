SHELL=/bin/bash -e

# In order to disable running unit tests, use this:
#	sbt "set test in assembly := {}" assembly
all:
	sbt assembly
	mkdir -p applet_resources/resources
	(DX_WDL_JAR=$$(find target -name "dxWDL.jar"); cp $$DX_WDL_JAR applet_resources/resources/dxWDL.jar)

test:
	sbt test

asset:
	dx build_asset applet_resources

clean:
	sbt clean clean-files
