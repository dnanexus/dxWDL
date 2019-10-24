# Procedures

## Release check list
- Make sure regression tests pass
- Update release notes and README.md
- Make sure the version number in `src/main/resources/application.conf` is correct. It is used
when building the release.
- Merge onto master branch, make sure [travis tests](https://travis-ci.org/dnanexus/dxWDL) pass

- Build new externally visible release
```
./scripts/build_all_releases.sh --staging-token=XXX --production-token=YYY --docker-user=UUU --docker-password=WWW
```
this will take a while. It builds the release
on staging, runs multi-region tests on staging, builds on production, and creates an easy to
use docker image.

- Update [releases](https://github.com/dnanexus-rnd/dxWDL/releases) github page, use the `Draft a new release` button, and upload a dxWDL.jar file.

## Post release
- Update the version number in `src/main/resources/application.conf`. We don't want
to mix the experimental release, with the old code.


## Coding guidelines

- Lines should not go over 100 characters
- Unit tests should assert, and not print to the console
- WDL test files belong in the top directory `test`
- No tabs in code, only spaces.
- Indentation in scala code should be 4 white spaces.

## Building

The main library we depend on is
[wdl4s](http://broadinstitute.github.io/wdl4s/latest/wdl4s/index.html). This
is a scala library for parsing and handling WDL programs, written at
the [Broad institute](https://www.broadinstitute.org).

The instructions here assume an Ubuntu 16.04 system (Xenial).

Install java v1.8
```
sudo apt install openjdk-8-jre-headless
```

Install Scala
```
wget www.scala-lang.org/files/archive/scala-2.12.1.deb
sudo dpkg -i scala-2.12.1.deb
```

Get ```sbt```, this is a make like utility that works with the ```scala``` language.
```
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install sbt
```

Running sbt for the first time takes several minutes, because it
downloads all required packages.

Checkout the code, and build it.
```
git clone https://github.com/dnanexus/dx-toolkit.git
git clone https://github.com/dnanexus-rnd/dxWDL.git
make -C dx-toolkit java
mkdir dxWDL/lib && cp dx-toolkit/lib/java/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar dxWDL/lib
cd dxWDL && make all
```

The dxWDL/lib subdirectory contains the java bindings for dnanexus,
the dxjava jar file. This allows the compilation process to find dx
methods. Now execute `./build_jar.py`, this will create a compiler jar
file and place it at the top level directory.

## SBT tips

### cache

sbt keeps the cache of downloaded jar files in
```${HOME}/.ivy2/cache```. For example, the WDL jar files are under
```${HOME}/.ivy2/cache/org.broadinstitute```. In case of problems with
cached jars, you can remove this directory recursively. This will make
WDL download all dependencies (again).
