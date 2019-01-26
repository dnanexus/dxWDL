#!/bin/bash -e

# Create a public docker image for the dxWDL compiler that allows a simple command line
# invocation:
#

# Find the root directory of the distribution, regardless of where
# the script is called from
base_dir=$(dirname "$0")
root_dir=$base_dir/../..

# Figure out the version number
conf_file=$root_dir/src/main/resources/application.conf
if [[ ! -e ${conf_file} ]]; then
    echo "Expecting the configuration file to be placed in ${conf_file}"
    exit 1
fi
version=$(grep version ${conf_file} | awk '{print $3}' | sed 's/\"//g' )
echo "dxwdl version is ${version}"

# Make sure the jar file exists
jar_file=$root_dir/dxWDL-${version}.jar
if [[ ! -e ${jar_file} ]]; then
    echo "jar file ${jar_file} is missing"
    exit 1
fi

# Make a copy of the jar file, because the docker image needs it to be
# -this- directory
rm -f dxWDL-${version}.jar
cp $jar_file dxWDL-${version}.jar

echo "building a docker image"
sudo docker build --build-arg VERSION=${version} -t dnanexus/dxwdl:${version} .

echo "tagging as latest"
sudo docker tag dnanexus/dxwdl:${version} dnanexus/dxwdl:latest

echo "For the next steps to work you need to:"
echo "(1) be logged into docker.io"
echo "(2) have permissions to create a repository for dnanexus"
echo "echo PASSWORD | sudo docker login -u USERNAME --password-stdin"
echo ""
echo "pushing to docker hub"
# sudo docker push dnanexus/dxwdl:${version}
# sudo docker push dnanexus/dxwdl:latest
