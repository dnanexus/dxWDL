#!/bin/bash -e

# https://stackoverflow.com/questions/4774054/reliable-way-for-a-bash-script-to-get-the-full-path-to-itself
# Get the source directory of the distribution
SCRIPT=`realpath $0`
SCRIPTPATH=`dirname $SCRIPT`
top_dir=$(realpath $SCRIPTPATH/..)
echo "The dxWDL top directory is: $top_dir"

# make sure dx is in our path
path_to_dx=$(which dx)
if [ -z "$path_to_dx" ] ; then
    echo "Could not find the dx CLI"
    exit 1
else
    echo "Found the dx CLI: $path_to_dx"
fi



# figure out the release tag
config=$top_dir/src/main/resources/application.conf
version=$(grep version src/main/resources/application.conf | cut --delimiter='"' --fields=2)
if [ -z $version ]; then
    echo "could not figure out the release version"
    exit 1
fi
echo "version is $version"


currentHash=$(git rev-parse HEAD)
possibleTags=$(git tag --contains $currentHash)
if [[ $possibleTags == "" ]]; then
    echo "setting release tag on github"
    git tag $version
    git push origin $version
else
    echo "Tag is already set"
fi

# build the release on staging
echo "building staging release"
dx login --staging --token $STAGING_TOKEN --noprojects
$top_dir/scripts/build_release.py --force --multi-region

## test that it actually works
echo "running multi region tests on staging"
$top_dir/scripts/multi_region_tests.py
$top_dir/scripts/proxy_test.py

echo "leave staging"
dx clearenv
dx logout

## build on production
echo "building on production"
dx login --token $PRODUCTION_TOKEN --noprojects
$top_dir/scripts/build_release.py --force --multi-region
