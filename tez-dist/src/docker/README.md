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

# Apache Tez Docker

This directory contains a unified Docker implementation for running TezAM
and TezChild process from a single container image. Based on
`TEZ_COMPONENT` environment variable the entrypoint is dynamically selected

1. Building the docker image:

   ```bash
   mvn clean install -DskipTests -Pdocker
   ```

   Alternatively, you can build it explicitly via the provided script:

   ```bash
   ./tez-dist/src/docker/build.sh -tez <version> -repo apache
   ```

2. Local Zookeeper Setup (Standalone):

   If you are running the AM container use the official Docker
   image (Refer to docker-compose.yml):

   ```bash
   docker pull zookeeper:3.8.4

   docker run -d \
       --name zookeeper-server \
       -p 2181:2181 \
       -p 8080:8080 \
       -e ZOO_MY_ID=1 \
       zookeeper:3.8.4
   ```

3. Running the Tez containers explicitly:

   **Running the Tez AM:**

   ```bash
   export TEZ_VERSION=1.0.0-SNAPSHOT

   docker run --rm \
       -p 10001:10001 \
       --env-file tez-dist/src/docker/am.env \
       --name tez-am \
       --hostname localhost \
       apache/tez:$TEZ_VERSION
   ```

   * `TEZ_VERSION` corresponds to the Maven `${project.version}`.
     Set this environment variable in your shell before running the commands.

   * Expose ports using the `-p` flag based on the
     `tez.am.client.am.port-range` property in `tez-site.xml`.

   * The `--hostname` flag configures the container's hostname, allowing
     services on the host (e.g., macOS) to connect to it.

   * Ensure the `--env-file` flag is included, or at a minimum, pass
     `-e TEZ_FRAMEWORK_MODE=STANDALONE_ZOOKEEPER` and `-e TEZ_COMPONENT=AM`
     to the `docker run` command.

   **Running the Tez Child:**

   The child container requires specific arguments (`<am-host> <am-port>
   <container-id> <token-id> <attempt-number>`) to connect back to the
   Application Master.

   Assuming your AM is running on `localhost` port `10001`, and the AM
   assigned the container ID `container_1703023223000_0001_01_000001`:

   ```bash
   docker run --rm \
       --network host \
       --env-file tez-dist/src/docker/child.env \
       --name tez-child \
       --hostname localhost \
       apache/tez:1.0.0-SNAPSHOT \
       localhost 10001 container_1703023223000_0001_01_000001 dummy_token_abc 1
   ```

4. Debugging the Tez containers:
   Uncomment the `JAVA_TOOL_OPTIONS` in `am.env` (or `child.env` for
   port 5006) and expose the debug port using `-p` flag:

   ```bash
   docker run --rm \
       -p 10001:10001 -p 5005:5005 \
       --env-file tez-dist/src/docker/am.env \
       --name tez-am \
       --hostname localhost \
       apache/tez:$TEZ_VERSION
   ```

5. To override the tez-site.xml in docker image use:

   * Set the `TEZ_CUSTOM_CONF_DIR` environment variable in `am.env` /
     `child.env` or via the `docker run` command (e.g.,
     `/opt/tez/custom-conf`).

   ```bash
   export TEZ_SITE_PATH=$(pwd)/tez-dist/src/docker/conf/tez-site.xml

   docker run --rm \
   -p 10001:10001 \
   --env-file tez-dist/src/docker/am.env \
   -v "$TEZ_SITE_PATH:/opt/tez/custom-conf/tez-site.xml" \
   --name tez-am \
   --hostname localhost \
   apache/tez:$TEZ_VERSION
   ```

6. To add plugin jars in docker image use:

   * The plugin directory path inside the Docker container is fixed at
     `/opt/tez/plugins`.

   ```bash
   docker run --rm \
   -p 10001:10001 \
   --env-file tez-dist/src/docker/am.env \
   -v "/path/to/your/local/plugins:/opt/tez/plugins" \
   --name tez-am \
   --hostname localhost \
   apache/tez:$TEZ_VERSION
   ```

7. Using Docker Compose (Local Testing Cluster):

   The provided `docker-compose.yml` offers a complete, minimal Hadoop
   ecosystem to test Tez in a distributed manner locally without setting
   up a real cluster.

   **Services Included:**

   * **namenode & datanode:** A minimal Apache Hadoop HDFS cluster
     (lean image)

   * **zookeeper:** Required by the Tez AM for standalone session
     discovery

   * **tez-am:** It automatically waits for Zookeeper and HDFS to
     be healthy before starting up.

   * **tez-child:**  TBD

   **To start the full cluster:**

   ```bash
   docker-compose -f tez-dist/src/docker/docker-compose.yml up -d
   ```

   **To monitor the Application Master logs:**

   ```bash
   docker-compose -f tez-dist/src/docker/docker-compose.yml logs -f tez-am
   ```

   **To shut down the cluster and clean up volumes (HDFS/Zookeeper data):**

   ```bash
   docker-compose -f tez-dist/src/docker/docker-compose.yml down -v
   ```

8. To mount custom plugins or JARs required by Tez AM (e.g., for split
   generation — typically the hive-exec jar, but in general, any UDFs or
   dependencies previously managed via YARN localization:

   * Create a directory `tez-plugins` and add all required jars.

   * Uncomment the following lines in docker compose under the `tez-am`
     and `tez-child` services to mount this directory as a volume to
     `/opt/tez/plugins` in the docker container.

     ```yaml
     volumes:
       - ./tez-plugins:/opt/tez/plugins
     ```
