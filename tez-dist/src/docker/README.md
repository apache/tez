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
    mvn clean install -DskipTests -Pdocker,tools
    ```

2. Install zookeeper in mac by:

    ```bash
    brew install zookeeper
    zkServer start
    ```

3. Running the Tez AM container:

    ```bash
    docker run \
        -p 10001:10001 -p 8042:8042 \
        --name tez-am \
        apache/tez-am:1.0.0-SNAPSHOT
    ```

4. Debugging the Tez AM container:
Uncomment the JAVA_TOOL_OPTIONS in tez.env and expose 5005 port using -p flag

    ```bash
    docker run --rm \
        -p 10001:10001 -p 8042:8042 -p 5005:5005 \
        -e TEZ_FRAMEWORK_MODE="STANDALONE_ZOOKEEPER" \
        --env-file tez.env \
        --name tez-am \
        apache/tez-am:1.0.0-SNAPSHOT
    ```

5. To override the tez-site.xml in docker image use:

```bash
    docker run --rm \
        -p 10001:10001 -p 8042:8042 -p 5005:5005 \
        -e TEZ_FRAMEWORK_MODE="STANDALONE_ZOOKEEPER" \
        --env-file tez.env \
        -v "$(pwd)/conf/tez-site.xml:/opt/tez/custom-conf/tez-site.xml" \
        --name tez-am \
        apache/tez-am:1.0.0-SNAPSHOT
    ```
