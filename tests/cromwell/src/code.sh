#!/bin/bash -ex

main() {
    # Download test data
    dx download -r project-F2gg1xj0KFB7BqB65QJG3GY3:/genomics-public-data/
    dx download -r project-F2gg1xj0KFB7BqB65QJG3GY3:/cromwell-comp

    sudo apt-get update

    # Install docker
    curl -sSL https://get.docker.com/ | sh

    # Install java 8
    apt get install -y openjdk-8-jre-headless
    java -version

    # Download cromwell
    wget https://github.com/broadinstitute/cromwell/releases/download/25/cromwell-25.jar
    java -jar cromwell-25.jar run simple.wdl

    # Testing
    sudo docker run hello-world

    # Allow running docker in user mode
    sudo usermod -aG docker $USER
    docker run hello-world

    # Sleep for 7 days. This allows logging into the machine,
    # and working interactively.
    # 7 * 24 * 60 * 60  seconds
    sleep(604800)
}
