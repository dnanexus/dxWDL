FROM ubuntu:18.04
SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND noninteractive
ENTRYPOINT ["/bin/bash"]

# Install a few packages, to allow non trivial testing
RUN apt-get update && \
    apt-get install -y autoconf apt-utils make curl \
          tzdata git tree wget ispell lsof sudo
