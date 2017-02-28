#!/bin/bash -e

# Install scala 2.11.8
wget www.scala-lang.org/files/archive/scala-2.11.8.deb
sudo dpkg -i scala-2.11.8.deb

# Install sbt
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt-get update
sudo apt-get install sbt

# Clone and make the dx java library
git clone https://github.com/dnanexus/dx-toolkit.git
make -C dx-toolkit java

# Clone dxWDL
git clone https://github.com/dnanexus-rnd/dxWDL.git
mkdir -p dxWDL/lib
cp dx-toolkit/lib/java/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar dxWDL/lib
