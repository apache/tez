#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

ARG BUILD_ENV=unarchive

# hadolint ignore=DL3006
FROM ubuntu AS unarchive
# hadolint ignore=DL3010
ONBUILD COPY hadoop-*.tar.gz /opt
# hadolint ignore=DL3010
ONBUILD COPY tez-*.tar.gz /opt

# hadolint ignore=DL3006
FROM ${BUILD_ENV} AS env
ARG HADOOP_VERSION
ARG TEZ_VERSION

RUN mkdir -p /opt/hadoop \
    && tar -xzv \
        --exclude="hadoop-$HADOOP_VERSION/share/doc" \
        --exclude="*/jdiff" \
        --exclude="*/sources" \
        --exclude="*tests.jar" \
        --exclude="*/webapps" \
        -f /opt/hadoop-$HADOOP_VERSION.tar.gz \
        -C /opt/hadoop --strip-components 1 \
    && mkdir -p /opt/tez \
    && tar -xzv \
        -f /opt/tez-$TEZ_VERSION.tar.gz \
        -C /opt/tez \
    && rm -rf /opt/hadoop-$HADOOP_VERSION.tar.gz /opt/tez-$TEZ_VERSION.tar.gz

FROM eclipse-temurin:21-jdk-ubi9-minimal AS run

ARG UID=1000
ARG HADOOP_VERSION
ARG TEZ_VERSION

# Install dependencies
# hadolint ignore=DL3041
RUN set -ex; \
    microdnf update -y; \
    microdnf -y install procps gettext findutils hostname; \
    microdnf clean all; \
    useradd --no-create-home -s /sbin/nologin -c "" --uid $UID tez

# Set necessary environment variables
ENV HADOOP_HOME=/opt/hadoop \
    TEZ_HOME=/opt/tez \
    TEZ_CONF_DIR=/opt/tez/conf \
    HADOOP_CONF_DIR=/opt/tez/conf

ENV TEZ_CLIENT_VERSION=$TEZ_VERSION

ENV PATH=$TEZ_HOME/bin:$HADOOP_HOME/bin:$PATH

COPY --from=env --chown=tez /opt/hadoop $HADOOP_HOME
# UPDATED: Copy from the normalized directory name created in 'env' stage
COPY --from=env --chown=tez /opt/tez $TEZ_HOME

RUN mkdir -p $TEZ_CONF_DIR && chown tez:tez $TEZ_CONF_DIR

COPY --chown=tez tez-am-entrypoint.sh /
COPY --chown=tez conf $TEZ_CONF_DIR

# Create Extension Point Directory
RUN mkdir -p /opt/tez/plugins && chown tez:tez /opt/tez/plugins && chmod 755 /opt/tez/plugins

RUN chmod +x /tez-am-entrypoint.sh

USER tez
WORKDIR $TEZ_HOME

# Expose AM ports via -p flag in docker command
# EXPOSE 10001 10002 10003 8042

ENTRYPOINT ["/tez-am-entrypoint.sh"]
