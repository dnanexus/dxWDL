#!/bin/bash -ex

main() {
    sudo apt-get update

    # Install docker
    curl -sSL https://get.docker.com/ | sh

    # Install java 8
    apt-get install -y openjdk-8-jre-headless
    java -version

    # Download cromwell
    wget https://github.com/broadinstitute/cromwell/releases/download/26/cromwell-26.jar
    java -jar cromwell-26.jar run simple.wdl

    # Testing
    sudo docker run hello-world

    # Allow running docker in user mode
    sudo usermod -aG docker $USER
    docker run hello-world

    # download GATK pipeline
    wget https://github.com/broadinstitute/wdl/blob/develop/scripts/broad_pipelines/PublicPairedSingleSampleWf_170412.wdl
    wget https://github.com/broadinstitute/wdl/blob/develop/scripts/broad_pipelines/PublicPairedSingleSampleWf_170412.inputs.json

    # Sleep for 7 days. This allows logging into the machine,
    # and working interactively.
    # 7 * 24 * 60 * 60  seconds
    sleep(604800)
}
