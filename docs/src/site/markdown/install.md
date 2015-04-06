<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

<head><title>Install and Deployment Instructions</title></head>

Install/Deploy Instructions for Tez
---------------------------------------------------------------------------
Replace x.y.z with the tez release number that you are using. E.g. 0.5.0

1.  Deploy Apache Hadoop using either the 2.2.0 release or a compatible 2.x version.
    -   You need to change the value of the hadoop.version property in the
        top-level pom.xml to match the version of the hadoop branch being used.
        ```
        $ hadoop version
        ```
2.  Build tez using `mvn clean package -DskipTests=true -Dmaven.javadoc.skip=true`
    -   This assumes that you have already installed JDK6 or later and Maven 3 or later.
    -   Tez also requires Protocol Buffers 2.5.0, including the protoc-compiler.
        *   This can be downloaded from https://github.com/google/protobuf/tags/.
        *   On Mac OS X with the homebrew package manager `brew install protobuf250`
        *   For rpm-based linux systems, the yum repos may not have the 2.5.0 version.
            `rpm.pbone.net` has the protobuf-2.5.0 and protobuf-compiler-2.5.0 packages.
    -   If you prefer to run the unit tests, remove skipTests from the
        command above.
    -   If you use Eclipse IDE, you can import the projects using
        "Import/Maven/Existing Maven Projects". Eclipse does not
        automatically generate Java sources or include the generated
        sources into the projects. Please build using maven as described
        above and then use Project Properties to include
        "target/generatedsources/java" as a source directory into the
        "Java Build Path" for these projects: tez-api, tez-mapreduce,
        tez-runtime-internals and tez-runtime-library. This needs to be done
        just once after importing the project.
3.  Copy the relevant tez tarball into HDFS, and configure tez-site.xml
    -   A tez tarball containing tez and hadoop libraries will be found
        at tez-dist/target/tez-x.y.z-SNAPSHOT.tar.gz
    -   Assuming that the tez jars are put in /apps/ on HDFS, the
        command would be
        ```
            hadoop dfs -mkdir /apps/tez-x.y.z-SNAPSHOT
            hadoop dfs -copyFromLocal tez-dist/target/tez-x.y.z-SNAPSHOT-archive.tar.gz /apps/tez-x.y.z-SNAPSHOT/
        ```
    -   tez-site.xml configuration.
        -   Set tez.lib.uris to point to the tar.gz uploaded to HDFS.
            Assuming the steps mentioned so far were followed,
            ```
            set tez.lib.uris to "${fs.defaultFS}/apps/tez-x.y.z-SNAPSHOT/tez-x.y.z-SNAPSHOT.tar.gz"
            ```
        -   Ensure tez.use.cluster.hadoop-libs is not set in tez-site.xml,
            or if it is set, the value should be false
    -  Please note that the tarball version should match the version of
       the client jars used when submitting Tez jobs to the cluster.
       Please refer to the [Version Compatibility Guide](https://cwiki.apache.org/confluence/display/TEZ/Version+Compatibility)
       for more details on version compatibility and detecting mismatches.
4.  Optional: If running existing MapReduce jobs on Tez. Modify
    mapred-site.xml to change "mapreduce.framework.name" property from
    its default value of "yarn" to "yarn-tez"
5.  Configure the client node to include the tez-libraries in the hadoop
    classpath
    -   Extract the tez minimal tarball created in step 2 to a local directory
        (assuming TEZ_JARS is where the files will be decompressed for
        the next steps)
        ```
        tar -xvzf tez-dist/target/tez-x.y.z-minimal.tar.gz -C $TEZ_JARS
        ```
    -   set TEZ_CONF_DIR to the location of tez-site.xml
    -   Add $TEZ_CONF_DIR, ${TEZ_JARS}/* and ${TEZ_JARS}/lib/* to the application classpath.
        For example, doing it via the standard Hadoop tool chain would use the following command
	to set up the application classpath:
        ```
        export HADOOP_CLASSPATH=${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*
        ```
    -   Please note the "*" which is an important requirement when
        setting up classpaths for directories containing jar files.
6.  There is a basic example of using an MRR job in the tez-examples.jar.
    Refer to OrderedWordCount.java in the source code. To run this
    example:

    ```
    $HADOOP_PREFIX/bin/hadoop jar tez-examples.jar orderedwordcount <input> <output>
    ```

    This will use the TEZ DAG ApplicationMaster to run the ordered word
    count job. This job is similar to the word count example except that
    it also orders all words based on the frequency of occurrence.

    Tez DAGs could be run separately as different applications or
    serially within a single TEZ session. There is a different variation
    of orderedwordcount in tez-tests that supports the use of Sessions
    and handling multiple input-output pairs. You can use it to run
    multiple DAGs serially on different inputs/outputs.

    ```
    $HADOOP_PREFIX/bin/hadoop jar tez-tests.jar testorderedwordcount <input1> <output1> <input2> <output2> <input3> <output3> ...
    ```

    The above will run multiple DAGs for each input-output pair.

    To use TEZ sessions, set -DUSE_TEZ_SESSION=true

    ```
    $HADOOP_PREFIX/bin/hadoop jar tez-tests.jar testorderedwordcount -DUSE_TEZ_SESSION=true <input1> <output1> <input2> <output2>
    ```
7.  Submit a MR job as you normally would using something like:

    ```
    $HADOOP_PREFIX/bin/hadoop jar hadoop-mapreduce-client-jobclient-3.0.0-SNAPSHOT-tests.jar sleep -mt 1 -rt 1 -m 1 -r 1
    ```

    This will use the TEZ DAG ApplicationMaster to run the MR job. This
    can be verified by looking at the AM’s logs from the YARN ResourceManager UI.
    This needs mapred-site.xml to have "mapreduce.framework.name" set to "yarn-tez"

Hadoop Installation dependent Install/Deploy Instructions
---------------------------------------------------------
The above install instructions use Tez with pre-packaged Hadoop libraries included in the package and is the
recommended method for installation. If its needed to make Tez use the existing cluster Hadoop libraries then
follow this alternate machanism to setup Tez to use Hadoop libraries from the cluster.
Step 3 above changes as follows. Also subsequent steps would use tez-dist/target/tez-x.y.z-minimal.tar.gz instead of tez-dist/target/tez-x.y.z.tar.gz
- A tez build without Hadoop dependencies will be available at tez-dist/target/tez-x.y.z-minimal.tar.gz
- Assuming that the tez jars are put in /apps/ on HDFS, the command would be
"hadoop fs -mkdir /apps/tez-x.y.z"
"hadoop fs -copyFromLocal tez-dist/target/tez-x.y.z-minimal.tar.gz /apps/tez-x.y.z"
- tez-site.xml configuration
- Set tez.lib.uris to point to the paths in HDFS containing the tez jars. Assuming the steps mentioned so far were followed,
set tez.lib.uris to "${fs.defaultFS}/apps/tez-x.y.z/tez-x.y.z-minimal.tar.gz
- set tez.use.cluster.hadoop-libs to true


[Install instructions for older versions of Tez (pre 0.5.0)](./install_pre_0_5_0.html)
-----------------------------------------------------------------------------------

