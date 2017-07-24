SHELL=/bin/bash -e

all:
	mkdir -p applet_resources/resources
	sbt assembly

test:
	sbt test

clean:
	find . -name "dxWDL*.jar" | xargs rm -f
	rm -f src/main/resources/*.conf
	sbt clean clean-files
