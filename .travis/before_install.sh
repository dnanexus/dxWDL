#!/bin/bash -ex

# Install scala 2.11.8
wget www.scala-lang.org/files/archive/scala-2.11.8.deb
sudo dpkg -i scala-2.11.8.deb
rm scala-2.11.8.deb

# Install sbt
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install sbt

# Clone and make the python and java dxpy version
git clone https://github.com/dnanexus/dx-toolkit.git
make -C dx-toolkit python java

# Clone dxWDL into a new directory, NOT called dxWDL
pwd
mkdir TLC
cd TLC
git clone https://github.com/dnanexus-rnd/dxWDL.git
ls -R dxWDL
mkdir -p dxWDL/lib
cp ../dx-toolkit/lib/java/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar dxWDL/lib/
