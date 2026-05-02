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

# === DEPENDENCY VERSIONS & HASHES ===
ARG ALPINE_VERSION="3.23.4"
ARG PROTOBUF_VERSION="25.5"
ARG SHELLCHECK_VERSION="0.11.0"
ARG SHELLCHECK_SHA="7c80e0a8fe92dd73096b99f1e0aad9dcda119b69b8087445785a9c7c1b57dff882d3480bad6fef777b215f98785e1233a28a474420fc3819085138081953e58a"
ARG HADOLINT_VERSION="2.14.0"
ARG HADOLINT_SHA="5ffd7ed8f27894941a82f06229ed0dc75814eeb985d224d4fc3c7cf516f31cc7e6cc2d57348d4026084ac622c765e63c8274fdc6c36c0de03c4a5dda8f4ebf6f"
ARG BUF_VERSION="1.68.2"
ARG BUF_SHA="557ea42d00458466e3421bd1cf5781d882a95b0c1c0e54efffc326fdf9993d02"
ARG SPOTBUGS_VERSION="4.9.3"
ARG MAVEN_VERSION="3.9.15"
ARG ASTROID_VERSION="4.0.3"
ARG PYLINT_VERSION="4.0.4"
ARG CODESPELL_VERSION="2.4.1"
ARG YAMLLINT_VERSION="1.38.0"
ARG JSHINT_VERSION="2.13.6"
ARG MARKDOWNLINT_VERSION="0.46.0"

#############################################################
# PARALLEL DOWNLOAD STAGES (using lightweight Alpine image) #
#############################################################

#########################
# Fetch Google Protobuf #
#########################
FROM alpine:${ALPINE_VERSION} AS protobuf
ARG PROTOBUF_VERSION
# hadolint ignore=DL3018
RUN apk add --no-cache curl unzip \
    && mkdir -p /opt/protobuf \
    && curl -L -s -S https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip -o /opt/protoc.zip \
    && unzip -q /opt/protoc.zip -d /opt/protobuf \
    && rm /opt/protoc.zip

########################################
# Fetch shellcheck (shell script lint) #
########################################
FROM alpine:${ALPINE_VERSION} AS shellcheck
ARG SHELLCHECK_VERSION
ARG SHELLCHECK_SHA
SHELL ["/bin/ash", "-o", "pipefail", "-c"]
# hadolint ignore=DL3018
RUN apk add --no-cache curl tar xz \
    && curl -sSL https://github.com/koalaman/shellcheck/releases/download/v${SHELLCHECK_VERSION}/shellcheck-v${SHELLCHECK_VERSION}.linux.x86_64.tar.xz | tar --strip-components 1 --wildcards -xJf - '*/shellcheck' \
    && chmod a+rx shellcheck \
    && mv shellcheck /bin/shellcheck \
    && echo "${SHELLCHECK_SHA}  /bin/shellcheck" | sha512sum -c -

####################################
# Fetch hadolint (dockerfile lint) #
####################################
FROM alpine:${ALPINE_VERSION} AS hadolint
ARG HADOLINT_VERSION
ARG HADOLINT_SHA
SHELL ["/bin/ash", "-o", "pipefail", "-c"]
# hadolint ignore=DL3018
RUN apk add --no-cache curl \
    && curl -sSL https://github.com/hadolint/hadolint/releases/download/v${HADOLINT_VERSION}/hadolint-Linux-x86_64 -o /bin/hadolint \
    && chmod a+rx /bin/hadolint \
    && echo "${HADOLINT_SHA}  /bin/hadolint" | sha512sum -c -

#############################
# Fetch buf (protobuf lint) #
#############################
FROM alpine:${ALPINE_VERSION} AS buf
ARG BUF_VERSION
ARG BUF_SHA
SHELL ["/bin/ash", "-o", "pipefail", "-c"]
# hadolint ignore=DL3018
RUN apk add --no-cache curl tar \
    && curl -sSL https://github.com/bufbuild/buf/releases/download/v${BUF_VERSION}/buf-Linux-x86_64.tar.gz -o buf.tar.gz \
    && echo "${BUF_SHA}  buf.tar.gz" | sha256sum -c - \
    && tar -xzf buf.tar.gz -C /usr/local --strip-components 1 \
    && rm buf.tar.gz

####################
# MAIN BUILD STAGE #
####################

# Switching to eclipse-temurin to resolves cross-platform amd64/arm64 architecture issues
FROM eclipse-temurin:21-jdk-noble AS tezbase

WORKDIR /root
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV DEBIAN_FRONTEND=noninteractive
ENV DEBCONF_TERSE=true

# Re-declare arguments needed in the final stage
ARG SPOTBUGS_VERSION
ARG MAVEN_VERSION
ARG ASTROID_VERSION
ARG PYLINT_VERSION
ARG CODESPELL_VERSION
ARG YAMLLINT_VERSION
ARG JSHINT_VERSION
ARG MARKDOWNLINT_VERSION

##############################################
# Install OS dependencies and C/C++ compiler #
##############################################
# hadolint ignore=DL3008
RUN apt-get -q update && apt-get -q install --no-install-recommends -y \
    apt-transport-https \
    apt-utils \
    bzip2 \
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
    unzip \
    xz-utils \
    g++ \
    gcc \
    libc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

##################
# Set the locale #
##################
RUN locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

####################
# Install SpotBugs #
####################
RUN mkdir -p /opt/spotbugs \
    && curl -L -s -S https://github.com/spotbugs/spotbugs/releases/download/${SPOTBUGS_VERSION}/spotbugs-${SPOTBUGS_VERSION}.tgz \
      -o /opt/spotbugs.tgz \
    && tar xzf /opt/spotbugs.tgz --strip-components 1 -C /opt/spotbugs \
    && chmod +x /opt/spotbugs/bin/*
ENV SPOTBUGS_HOME=/opt/spotbugs

#################
# Install Maven #
#################
RUN mkdir -p /opt/maven \
    && curl -L -s -S https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
      -o /opt/maven.tar.gz \
    && tar xzf /opt/maven.tar.gz --strip-components 1 -C /opt/maven \
    && ln -s /opt/maven/bin/mvn /usr/bin/mvn \
    && rm /opt/maven.tar.gz
ENV MAVEN_HOME=/opt/maven

######################################################################
# Install python3 and pylint4                                        #
# astroid and pylint go hand-in-hand. Upgrade both at the same time. #
######################################################################
# hadolint ignore=DL3008,DL3013
RUN apt-get -q update && apt-get -q install --no-install-recommends -y \
        python3 \
        python3-bcrypt \
        python3-cffi \
        python3-cryptography \
        python3-dateutil \
        python3-dev \
        python3-isort \
        python3-dockerpty \
        python3-nacl \
        python3-pyrsistent \
        python3-setuptools \
        python3-singledispatch \
        python3-six \
        python3-wheel \
        python3-wrapt \
        python3-yaml \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py \
    && python3 /tmp/get-pip.py --break-system-packages \
    && rm /usr/local/bin/pip /tmp/get-pip.py \
    && pip3 install --no-cache-dir --break-system-packages -v \
        astroid==${ASTROID_VERSION} \
        codespell==${CODESPELL_VERSION} \
        pylint==${PYLINT_VERSION} \
        yamllint==${YAMLLINT_VERSION} \
    && mv /usr/local/bin/pylint /usr/local/bin/pylint4 \
    && ln -s /usr/local/bin/pylint4 /usr/local/bin/pylint \
    && ln -s /usr/local/bin/pip3 /usr/local/bin/pip

##########################
# Install npm and JSHint #
##########################
# hadolint ignore=DL3008
RUN apt-get update && apt-get install --no-install-recommends -y nodejs npm \
    && npm install -g \
        jshint@${JSHINT_VERSION} \
        markdownlint-cli@${MARKDOWNLINT_VERSION} \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /root/.npm

################################
# Copy over parallel downloads #
################################
COPY --from=shellcheck /bin/shellcheck /bin/shellcheck
COPY --from=hadolint /bin/hadolint /bin/hadolint
COPY --from=buf /usr/local/bin/buf /usr/local/bin/buf
COPY --from=protobuf /opt/protobuf /opt/protobuf

ENV PROTOBUF_HOME=/opt/protobuf
ENV PROTOC_PATH=/opt/protobuf/bin/protoc
ENV PATH="${PATH}:/opt/protobuf/bin"

####
# YETUS CUT HERE
# Magic text above! Everything from here on is ignored
# by Yetus, so could include anything not needed
# by your testing environment
###
