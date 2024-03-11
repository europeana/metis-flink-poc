FROM flink:1.18.1-java21_poc

MAINTAINER PSNC

RUN apt-get update && apt-get install -y \
    xz-utils \
    telnet  \
    iputils-ping    \
    procps \
    traceroute \
    curl \
    wget \
    net-tools \
    dnsutils \
    vim \
    && rm -rf /var/lib/apt/lists/*

COPY install.sh /

RUN  chmod 555 /install.sh
RUN  /install.sh

RUN apt-get update && apt-get install -y \
    zip \
    unzip \
    lsof \
    && rm -rf /var/lib/apt/lists/*

COPY jattach /usr/bin/
RUN chmod 755 /usr/bin/jattach