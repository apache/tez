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

Apache Tez
==========

Apache Tez is a generic data-processing pipeline engine envisioned as a
low-level engine for higher abstractions such as Apache Hive, Apache Pig etc.

At its heart, tez is very simple and has just two components:

* The data-processing pipeline engine where-in one can plug-in input,
    processing and output implementations to perform arbitrary data-processing.
    Every 'task' in tez has the following:

* Input to consume key/value pairs from.
* Processor to process them.
* Output to collect the processed key/value pairs.

* A master for the data-processing application, where-by one can put together
   arbitrary data-processing 'tasks' described above into a task-DAG to process
   data as desired.
   The generic master is implemented as a Apache Hadoop YARN ApplicationMaster.

Building Tez
------------

For instructions on how to contribute to Tez, refer to:
[Tez Wiki - How to Contribute](https://cwiki.apache.org/confluence/display/TEZ)

Requirements
------------

* JDK 21+
* Maven 3.9.14 or later
* spotbugs 4.9.3 or later (if running spotbugs)
* ProtocolBuffer 3.25.5
* Hadoop 3.x

Maven Build Goals
-----------------

* Clean: `mvn clean`
* Compile: `mvn compile`
* Run tests: `mvn test`
* Create JAR: `mvn package`
* Run spotbugs: `mvn compile spotbugs:spotbugs`
* Run checkstyle: `mvn compile checkstyle:checkstyle`
* Install JAR in M2 cache: `mvn install`
* Deploy JAR to Maven repo: `mvn deploy`
* Run jacoco: `mvn test -Pjacoco`
* Run Rat: `mvn apache-rat:check`
* Build javadocs: `mvn javadoc:javadoc`
* Build distribution: `mvn package -Dhadoop.version=3.4.2`
* Visualize state machines: `mvn compile -Pvisualize -DskipTests=true`

Build Options
-------------

* Use `-Dpackage.format` to create distributions with a format other than
   .tar.gz (mvn-assembly-plugin formats).
* Use `-Dhadoop.version` to specify the version of Hadoop to build Tez against.
* Use `-Dprotoc.path` to specify the path to `protoc`.
* Use `-Dallow.root.build` to root build `tez-ui` components.

Building against a Specific Version of Hadoop
----------------------------------------------

Tez runs on top of Apache Hadoop YARN and requires Hadoop 3.x.

By default, it can be compiled against other compatible Hadoop versions by
specifying `hadoop.version`:

```bash
mvn package -Dhadoop.version=3.4.2
```

For recent versions of Hadoop (which do not bundle AWS and Azure by default),
you can bundle AWS-S3 or Azure support:

```bash
mvn package -Dhadoop.version=3.4.2 -Paws -Pazure
```

Tez also has shims to provide version-specific implementations for various APIs.
For more details, refer to
[Hadoop Shims](https://cwiki.apache.org/confluence/display/TEZ/HadoopShims).

Tez UI
------

* **UI Build Issues**

  In case of issues with the UI build, please clean the UI cache:

  ```bash
  mvn clean -PcleanUICache
  ```

* **Skip UI Build**

  To skip the UI build, use the `noui` profile:

  ```bash
  mvn clean install -DskipTests -Pnoui
  ```

  Maven will still include the `tez-ui` project, but all related plugins will be
  skipped.

* **Issue with PhantomJS on building in PowerPC**

  Official PhantomJS binaries were not available for the Power platform. If the
  build fails on PPC, try installing PhantomJS manually and rerun. Refer to
  [PhantomJS README](https://github.com/ibmsoe/phantomjs-1/blob/v2.1.1-ppc64/README.md)
  and install it globally.

Protocol Buffer Compiler
------------------------

The version of the Protocol Buffer compiler (`protoc`) can be defined
on-the-fly:

```bash
mvn clean install -DskipTests -pl ./tez-api -Dprotobuf.version=3.25.5
```

The default version is defined in the root `pom.xml`.

If you have multiple versions of `protoc`, set the `PROTOC_PATH` environment
variable to point to the desired binary. If not defined, the embedded `protoc`
compiler corresponding to `${protobuf.version}` will be used.

Alternatively, specify the path during the build:

```bash
mvn package -DskipTests -Dprotoc.path=/usr/local/bin/protoc
```

Building the Docs
-----------------

Build a local copy of the Apache Tez website:

```bash
mvn site -pl docs
```

Building Components Separately
------------------------------

If you are building a submodule directory, dependencies will be resolved from
the Maven cache or remote repositories. Alternatively, run
`mvn install -DskipTests` from the Tez top level once and then work from the
submodule.

Visualize State Machines
------------------------

Use `-Pvisualize` to generate a Graphviz file (`Tez.gv`) representing state
transitions:

```bash
mvn compile -Pvisualize -DskipTests=true
```

Optional parameters:

* `-Dtez.dag.state.classes=<comma-separated list of classes>`
    (Default: DAG, Vertex, Task, TaskAttempt)
* `-Dtez.graphviz.title` (Default: Tez)
* `-Dtez.graphviz.output.file` (Default: `tez-dag/target/Tez.gv`)

Example for `DAGImpl`:

```bash
mvn compile -Pvisualize \
  -Dtez.dag.state.classes=org.apache.tez.dag.app.dag.impl.DAGImpl \
  -DskipTests=true
```

Convert the `.gv` file to an image:

```bash
dot -Tpng -o Tez.png tez-dag/target/Tez.gv
```

Building Contrib Tools
----------------------

Use `-Ptools` to build tools under `tez-tools`:

```bash
mvn package -Ptools
```
