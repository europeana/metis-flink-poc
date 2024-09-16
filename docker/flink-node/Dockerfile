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

#Kubernetes client - kubectl - for better diagnostic in case of problems
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl.sha256" \
    && echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check \
    && install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

#openshift client - oc - for better diagnostic in case of problems
RUN mkdir /oc-temp \
    && cd /oc-temp \
    && curl -LO https://github.com/okd-project/okd/releases/download/4.15.0-0.okd-2024-03-10-010116/openshift-client-linux-4.15.0-0.okd-2024-03-10-010116.tar.gz \
    && tar -xzf openshift-client-linux-4.15.0-0.okd-2024-03-10-010116.tar.gz \
    && mv oc /usr/bin \
    && rm -rf /oc-temp \
