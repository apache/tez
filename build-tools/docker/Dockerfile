#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

###############
#
# Apache Yetus Dockerfile for Apache Tez
# NOTE: This file is compatible with Docker BuildKit. It will work
#       with standard docker build, but it is a lot faster
#       if BuildKit is enabled.
#
###############

FROM ubuntu:focal AS tezbase

WORKDIR /root
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_TERSE true

######
# Install some basic Apache Yetus requirements
# some git repos need ssh-client so do it too
# Adding libffi-dev for all the programming languages
# that take advantage of it.
######
# hadolint ignore=DL3008
RUN apt-get -q update && apt-get -q install --no-install-recommends -y \
    apt-transport-https \
    apt-utils \
    ca-certificates \
    curl \
    dirmngr \
    git \
    gpg \
    gpg-agent \
    libffi-dev \
    locales \
    make \
    pkg-config \
    rsync \
    software-properties-common \
    ssh-client \
    xz-utils \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

###
# Set the locale
###
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

####
# Install GNU C/C++ (everything generally needs this)
####
# hadolint ignore=DL3008
RUN apt-get -q update && apt-get -q install --no-install-recommends -y \
        g++ \
        gcc \
        libc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

###
# Install golang as part of base so we can do each
# helper utility in parallel. go bins are typically
# statically linked, so this is perfectly safe.
###
# hadolint ignore=DL3008
RUN add-apt-repository -y ppa:longsleep/golang-backports \
    && apt-get -q update \
    && apt-get -q install --no-install-recommends -y golang-go \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

############
# Fetch all of the non-conflicting bits in parallel
#############

######
# Install Google Protobuf 2.5.0
######
FROM tezbase AS protobuf250
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN mkdir -p /opt/protobuf-src \
    && curl -L -s -S \
      https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz \
      -o /opt/protobuf.tar.gz \
    && tar xzf /opt/protobuf.tar.gz --strip-components 1 -C /opt/protobuf-src
WORKDIR /opt/protobuf-src
RUN  ./configure --prefix=/opt/protobuf \
    && make install
WORKDIR /root
RUN rm -rf /opt/protobuf-src

####
# Install shellcheck (shell script lint)
####
FROM tezbase AS shellcheck
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl -sSL \
    https://github.com/koalaman/shellcheck/releases/download/v0.7.1/shellcheck-v0.7.1.linux.x86_64.tar.xz \
        | tar --strip-components 1 --wildcards -xJf - '*/shellcheck' \
    && chmod a+rx shellcheck \
    && mv shellcheck /bin/shellcheck \
    && shasum -a 512 /bin/shellcheck \
    | awk '$1!="aae813283d49f18f95a205dca1c5184267d07534a08abc952ebea1958fee06f8a0207373b6770a083079ba875458ea9da443f2b9910a50dcd93b935048bc14f5" {exit(1)}'

####
# Install hadolint (dockerfile lint)
####
FROM tezbase AS hadolint
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl -sSL \
        https://github.com/hadolint/hadolint/releases/download/v1.18.0/hadolint-Linux-x86_64 \
        -o /bin/hadolint \
    && chmod a+rx /bin/hadolint \
    && shasum -a 512 /bin/hadolint \
    | awk '$1!="df27253d374c143a606483b07a26234ac7b4bca40b4eba53e79609c81aa70146e7d5c145f90dcec71d6d1aad1048b7d9d2de68d92284f48a735d04d19c5c5559" {exit(1)}'

####
# Install buf (protobuf lint)
####
FROM tezbase AS buf
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl -sSL \
      https://github.com/bufbuild/buf/releases/download/v0.21.0/buf-Linux-x86_64.tar.gz \
      -o buf.tar.gz \
    && shasum -a 256 buf.tar.gz \
    | awk '$1!="95aba62ac0ecc5a9120cc58c65cdcc85038633a816bddfe8398c5ae3b32803f1" {exit(1)}' \
    && tar -xzf buf.tar.gz -C /usr/local --strip-components 1 \
    && rm buf.tar.gz

########
#
#
# Content that needs to be installed in order due to packages...
#
#
########

FROM tezbase
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

####
# Install java (first, since we want to dicate what form of Java)
####

####
# OpenJDK 8
####
# hadolint ignore=DL3008
RUN apt-get -q update && apt-get -q install --no-install-recommends -y openjdk-8-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

####
# OpenJDK 11 (but keeps default to JDK8)
# NOTE: This default only works when Apache Yetus is launched
# _in_ the container and not outside of it!
####
# hadolint ignore=DL3008
RUN apt-get -q update && apt-get -q install --no-install-recommends -y default-jre-headless openjdk-11-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && update-java-alternatives -s java-1.8.0-openjdk-amd64 \
    && rm -f /usr/lib/jvm/default-java \
    && ln -s java-8-openjdk-amd64 /usr/lib/jvm/default-java
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

######
# Install findbugs
######
# hadolint ignore=DL3008
RUN apt-get -q update && apt-get -q install --no-install-recommends -y findbugs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV FINDBUGS_HOME /usr

######
# Install maven
######
# hadolint ignore=DL3008
RUN apt-get -q update && apt-get -q install --no-install-recommends -y maven \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

######
# Install python3 and pylint3
# astroid and pylint go hand-in-hand.  Upgrade both at the same time.
######
# hadolint ignore=DL3008,DL3013
RUN apt-get -q update && apt-get -q install --no-install-recommends -y \
        python3 \
        python3-bcrypt \
        python3-cffi \
        python3-cryptography \
        python3-dateutil \
        python3-dev \
        python3-dev \
        python3-isort \
        python3-dockerpty \
        python3-nacl \
        python3-pyrsistent \
        python3-setuptools \
        python3-setuptools \
        python3-singledispatch \
        python3-six \
        python3-wheel \
        python3-wrapt \
        python3-yaml \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py \
    && python3 /tmp/get-pip.py \
    && rm /usr/local/bin/pip /tmp/get-pip.py \
    && pip3 install -v \
        astroid==2.4.2 \
        codespell==2.0 \
        pylint==2.5.3 \
        yamllint==1.24.2 \
    && rm -rf /root/.cache \
    && mv /usr/local/bin/pylint /usr/local/bin/pylint3
RUN ln -s /usr/local/bin/pylint3 /usr/local/bin/pylint
RUN ln -s /usr/local/bin/pip3 /usr/local/bin/pip

###
# Install npm and JSHint
###
# hadolint ignore=DL3008
RUN curl -sSL https://deb.nodesource.com/setup_14.x | bash - \
    && apt-get -q install --no-install-recommends -y nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g \
        jshint@2.12.0 \
        markdownlint-cli@0.23.2 \
    && rm -rf /root/.npm

#####
# Now all the stuff that was built in parallel
#####

COPY --from=shellcheck /bin/shellcheck /bin/shellcheck
COPY --from=hadolint /bin/hadolint /bin/hadolint
COPY --from=buf /usr/local/bin/buf /usr/local/bin/buf
COPY --from=protobuf250 /opt/protobuf /opt/protobuf

ENV PROTOBUF_HOME /opt/protobuf
ENV PROTOC_PATH /opt/protobuf/bin/protoc
ENV PATH "${PATH}:/opt/protobuf/bin"

####
# YETUS CUT HERE
# Magic text above! Everything from here on is ignored
# by Yetus, so could include anything not needed
# by your testing environment
###
