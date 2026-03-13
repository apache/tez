<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Tez AM Docker

1. Building the docker image:

    ```bash
    mvn clean install -DskipTests -Pdocker
    ```

2. Install zookeeper in mac:

    a. Via brew: set the `tez.am.zookeeper.quorum` value as
    `host.docker.internal:2181` in `tez-site.xml`

    ```bash
    brew install zookeeper
    zkServer start
    ```

    b. Use Zookeeper docker image (Refer to docker compose yml):

    ```bash
    docker pull zookeeper:3.8.4

    docker run -d \
        --name zookeeper-server \
        -p 2181:2181 \
        -p 8080:8080 \
        -e ZOO_MY_ID=1 \
        zookeeper:3.8.4
    ```

3. Running the Tez AM container explicitly:

    ```bash
    export TEZ_VERSION=1.0.0-SNAPSHOT

    docker run --rm \
        -p 10001:10001 \
        --env-file tez-dist/src/docker/tez-am/tez-am.env \
        --name tez-am \
        --hostname localhost \
        apache/tez-am:$TEZ_VERSION
    ```

   * `TEZ_VERSION` corresponds to the Maven `${project.version}`.
       Set this environment variable in your shell before running the commands.
   * Expose ports using the `-p` flag based on the
     `tez.am.client.am.port-range` property in `tez-site.xml`.
   * The `--hostname` flag configures the container's hostname, allowing
      services on the host (e.g., macOS) to connect to it.
   * Ensure the `--env-file` flag is included, or at a minimum, pass
     `-e TEZ_FRAMEWORK_MODE=STANDALONE_ZOOKEEPER` to the `docker run` command.

4. Debugging the Tez AM container:
Uncomment the `JAVA_TOOL_OPTIONS` in `tez-am.env` and expose 5005 port
using `-p` flag

    ```bash
    docker run --rm \
        -p 10001:10001 -p 5005:5005 \
        --env-file tez-dist/src/docker/tez-am/tez-am.env \
        --name tez-am \
        --hostname localhost \
        apache/tez-am:$TEZ_VERSION
    ```

5. To override the tez-site.xml in docker image use:
   * Set the `TEZ_CUSTOM_CONF_DIR` environment variable in `tez-am.env`
      or via the `docker run` command (e.g., `/opt/tez/custom-conf`).

    ```bash
    export TEZ_SITE_PATH=$(pwd)/tez-dist/src/docker/conf/tez-site.xml

    docker run --rm \
    -p 10001:10001 \
    --env-file tez-dist/src/docker/tez-am/tez-am.env \
    -v "$TEZ_SITE_PATH:/opt/tez/custom-conf/tez-site.xml" \
    --name tez-am \
    --hostname localhost \
    apache/tez-am:$TEZ_VERSION
    ```

6. To add plugin jars in docker image use:
   * The plugin directory path inside the Docker container is fixed at `/opt/tez/plugins`.

    ```bash
    docker run --rm \
    -p 10001:10001 \
    --env-file tez-dist/src/docker/tez-am/tez-am.env \
    -v "/path/to/your/local/plugins:/opt/tez/plugins" \
    --name tez-am \
    --hostname localhost \
    apache/tez-am:$TEZ_VERSION
    ```

7. Using Docker Compose:
    * Refer to the `docker-compose.yml` file in this directory for
    an example of how to run both the Tez AM and Zookeeper containers
    together using Docker Compose.

     ```bash
     docker-compose -f tez-dist/src/docker/tez-am/docker-compose.yml up -d --build
     ```

    * This command will start both the Tez AM, Zookeeper, Minimal
    Hadoop containers as defined in the `docker-compose.yml` file.
