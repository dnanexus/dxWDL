#!/bin/bash -e

# Global variables
top_dir=""
version=""
dry_run=""
build_flags=""
staging_token=""
production_token=""
docker_password=""
docker_user=""

# https://stackoverflow.com/questions/4774054/reliable-way-for-a-bash-script-to-get-the-full-path-to-itself
# Get the source directory of the distribution
function get_top_dir {
    SCRIPT=`realpath $0`
    SCRIPTPATH=`dirname $SCRIPT`
    top_dir=$(realpath $SCRIPTPATH/..)
    echo "The dxWDL top directory is: $top_dir"
}

function get_version {
    # figure out the release tag
    config=$top_dir/src/main/resources/application.conf
    version=$(grep version src/main/resources/application.conf | cut --delimiter='"' --fields=2)
    if [ -z $version ]; then
        echo "could not figure out the release version"
        exit 1
    fi
    echo "version is $version"
}

function basic_checks {
    # make sure dx is in our path
    path_to_dx=$(which dx)
    if [ -z "$path_to_dx" ] ; then
        echo "Could not find the dx CLI"
        exit 1
    fi
    echo "Found the dx CLI: $path_to_dx"
}

function tag {
    currentHash=$(git rev-parse HEAD)
    possibleTags=$(git tag --contains $currentHash)
    if [[ $possibleTags == "" ]]; then
        echo "setting release tag on github"
        git tag $version
        git push origin $version
    else
        echo "Tag is already set"
    fi
}

function build {
    # build the release on staging
    echo "building staging release"
    dx login --staging --token $staging_token --noprojects
    $top_dir/scripts/build_release.py --multi-region $build_flags

    ## test that it actually works
    echo "running multi region tests on staging"
    $top_dir/scripts/multi_region_tests.py
    $top_dir/scripts/proxy_test.py

    echo "leave staging"
    dx clearenv

    ## build on production
    echo "building on production"
    dx login --token $production_token --noprojects
    $top_dir/scripts/build_release.py --multi-region $build_flags
}


# Create a public docker image for the dxWDL compiler that allows a simple command line
# invocation
function build_docker_image {
    cd $top_dir/scripts
    ln $top_dir/dxWDL-${version}.jar .

    echo "building a docker image"
    sudo docker build --build-arg VERSION=${version} -t dnanexus/dxwdl:${version} .

    echo "tagging as latest"
    sudo docker tag dnanexus/dxwdl:${version} dnanexus/dxwdl:latest

    echo "For the next steps to work you need to:"
    echo "(1) be logged into docker.io"
    echo "(2) have permissions to create a repository for dnanexus"
    echo $docker_password | sudo docker login -u $docker_user --password-stdin

    echo "pushing to docker hub"
    sudo docker push dnanexus/dxwdl:${version}
    sudo docker push dnanexus/dxwdl:latest
}

function usage_die
{
    echo "arguments: "
    echo "  --force: remove existing build artifacts, and build new ones"
    echo "  --dry-run: don't actually run anything"
    echo "  --staging-token <string>: an auth token for the staging environment"
    echo "  --production-token <string>: an auth token for the production environment"
    echo "  --docker-user <string>: docker user name"
    echo "  --docker-password <string>: docker password"
    exit 1
}

function parse_cmd_line {
    while [[ $# -ge 1 ]]
    do
        case "$1" in
            --force)
                build_flags="$build_flags --force"
                ;;
            --dry-run|--dry_run|--dryrun)
                dry_run=1
                build_flags="$build_flags --dry-run"
                ;;
            --staging-token)
                staging_token=$2
                shift
                ;;
            --production-token)
                production_token=$2
                shift
                ;;
            --docker-user)
                docker_user=$2
                shift
                ;;
            --docker-password)
                docker_password=$2
                shift
                ;;
            *)
                echo "unknown argument $1"
                usage_die
        esac
        shift
    done

    if [[ $dry_run == "1" ]]; then
        return
    fi

    if [[ $staging_token == "" ]]; then
        echo "staging token is missing"
        exit 1
    fi
    if [[ $production_token == "" ]]; then
        echo "production token is missing"
        exit 1
    fi
    if [[ $docker_user == "" ]]; then
        echo "docker user name is missing"
        exit 1
    fi
    if [[ $docker_password == "" ]]; then
        echo "docker password is missing"
        exit 1
    fi
}

# main program
basic_checks
parse_cmd_line $@
get_top_dir
get_version
tag
build
build_docker_image
