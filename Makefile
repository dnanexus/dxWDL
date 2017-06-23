SHELL=/bin/bash -e

all:
	mkdir -p applet_resources/resources
	sbt assembly

test:
	sbt test

clean:
	sbt clean clean-files
