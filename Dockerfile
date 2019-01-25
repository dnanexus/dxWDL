FROM ubuntu:16.04

# docker build -t dnanexus/dxwdl .

# 1. DNANexus SDK (dx-toolkit)
RUN apt-get update &&
    apt-get install -y openjdk-8-jdk-headless

# dxWDL
WORKDIR /
ARG DXWDL_VERSION=0.79
COPY dxWDL-$VERSION /dxWDL.jar
RUN chmod +x /dxWDL.jar

ENTRYPOINT ["java", "-jar", "/dxWDL.jar"]
