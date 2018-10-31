FROM ubuntu:16.04

# docker build -t dnanexus/dxwdl .

# 1. DNANexus SDK (dx-toolkit)
RUN apt-get update && apt-get install -y wget git openssl python python-dev g++ default-jdk
RUN wget https://wiki.dnanexus.com/images/files/dx-toolkit-v0.255.0-ubuntu-14.04-amd64.tar.gz && \
    tar -xzvf dx-toolkit-v0.255.0-ubuntu-14.04-amd64.tar.gz && \
    /bin/bash -c "source /dx-toolkit/environment"  
ENV PATH /dx-toolkit/bin:$PATH

# Python and pip (Python 2.7 is required)
RUN wget https://bootstrap.pypa.io/get-pip.py && \
    python get-pip.py && \
    pip install dxpy
RUN dx upgrade

# Upload Agent
WORKDIR /
RUN wget https://wiki.dnanexus.com/images/files/dnanexus-upload-agent-1.5.30-linux.tar.gz && \
    tar -xzvf dnanexus-upload-agent-1.5.30-linux.tar.gz && \
    cd dnanexus-upload-agent-1.5.30-linux
ENV PATH $PATH:/dnanexus-upload-agent-1.5.30-linux

# dxWDL
WORKDIR /
RUN wget https://github.com/dnanexus/dxWDL/releases/download/0.78/dxWDL-0.78.jar && \
    chmod +x dxWDL-0.78.jar

ENTRYPOINT ["java"]
CMD ["-jar", "/dxWDL-0.78.jar"]
