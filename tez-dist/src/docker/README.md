# Tez AM Docker
---

1. Building the docker image:
```bash
mvn clean install -DskipTests -Pdocker,tools
```
2. Install zookeeper in mac by
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
```bash
docker run \
        -p 10001:10001 -p 8042:8042 -p 5005:5005 \
        -e TEZ_FRAMEWORK_MODE="STANDALONE_ZOOKEEPER" \
        -e JAVA_TOOL_OPTIONS='-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005' \
        --name tez-am \
        apache/tez-am:1.0.0-SNAPSHOT
```
