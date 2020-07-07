FROM ubuntu:18.04
SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND noninteractive
ENTRYPOINT ["/bin/bash"]

# Install dx-toolkit
RUN apt-get update && \
    apt-get install -y autoconf apt-utils make curl \
          tzdata git \
          emacs-nox vim tree wget ispell lsof sudo \
          htop bc apt-transport-https lsb-release \
          libffi-dev python3 python3-pip python3-venv \
          libcurl4-openssl-dev zlib1g-dev \
          openssl libssl-dev openjdk-8-jdk-headless \
          fuse jq squid zip

# Get docker, so that Cromwell could run it
RUN curl -sSL https://get.docker.com/ | sh

# Get dependencies for running Go
RUN apt-get update && apt-get install -y wget git build-essential && \
    wget https://dl.google.com/go/go1.14.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.14.linux-amd64.tar.gz

# Set environment variables for Go
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"

# Install scala
RUN wget www.scala-lang.org/files/archive/scala-2.12.9.deb && \
    dpkg -i scala-2.12.9.deb && \
    rm -f scala-2.12.9.deb

# Install sbt
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
RUN apt-get update && \
    apt-get install -y sbt

# Set the language encoding to utf8
RUN apt-get install -y locales && \
    locale-gen "en_US.UTF-8"

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV LC_CTYPE=en_US.UTF-8

# Set python3 as default python implementation
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Setup time services
RUN ln -sf /usr/share/zoneinfo/America/Los_Angeles /etc/localtime

# Setup go directory
RUN mkdir -p /go && \
    chmod 755 /go

RUN pip3 install termcolor dxpy

# Install Go packages
RUN go get github.com/google/subcommands && \
    go get golang.org/x/sync/semaphore && \
    go install github.com/google/subcommands && \
    go get github.com/dnanexus/dxda && \
    go install github.com/dnanexus/dxda && \
    go install github.com/dnanexus/dxda/cmd/dx-download-agent && \
    go get -u github.com/jacobsa/fuse

# Obtain fresh repositories
RUN mkdir -p devel && cd devel && \
    git clone https://github.com/dnanexus/dx-toolkit.git && \
    git clone https://github.com/dnanexus/dxfuse.git

# move the dxfuse source code to the golang directory
# build the executable
RUN mv /devel/dxfuse /go/src/github.com/dnanexus && \
    go build -o /go/bin/dxfuse /go/src/github.com/dnanexus/dxfuse/cli/main.go

# Clone and make dxWDL
RUN git clone https://github.com/dnanexus/dxWDL.git
RUN cd /dxWDL && sbt assembly
