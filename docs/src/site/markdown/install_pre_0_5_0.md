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

[Install instructions for Tez (post 0.5.0)](./install.html)
-----------------------------------------------------------------------------------

Install/Deploy Instructions for Tez release pre-0.5.0 E.g. [(Tez-0.4.1)](http://archive.apache.org/dist/incubator/tez/tez-0.4.1-incubating/)
--------------------------------------------------------------------------------------------------------------------------------------------------

1.  Deploy Apache Hadoop using either the 2.2.0 release or a compatible
    2.x version.
    -   One thing to note though when compiling Tez is that you will
        need to change the value of the hadoop.version property in the
        toplevel pom.xml to match the version of the hadoop branch being
        used.
2.  Build tez using `mvn clean install -DskipTests=true -Dmaven.javadoc.skip=true`
    -   This assumes that you have already installed JDK6 or later,
        Maven 3 or later and Protocol Buffers (protoc compiler) 2.5 or
        later
    -   If you prefer to run the unit tests, remove skipTests from the
        command above.
    -   If you would like to create a tarball of the release, use `mvn
        clean package -Dtar -DskipTests=true -Dmaven.javadoc.skip=true`
    -   If you use Eclipse IDE, you can import the projects using
        "Import/Maven/Existing Maven Projects". Eclipse does not
        automatically generate Java sources or include the generated
        sources into the projects. Please build using maven as described
        above and then use Project Properties to include
        "target/generated-sources/java" as a source directory into the
        "Java Build Path" for these projects: tez-api, tez-mapreduce,
        tez-runtime-internals and tez-runtime-library. This needs to be done
        just once after importing the project.
3.  Copy the tez jars and their dependencies into HDFS.
    -   The tez jars and dependencies will be found in
        tez-dist/target/tez-0.4.1-incubating/tez-0.4.1-incubating if you run
        the intial command mentioned in step 2.
    -   Assuming that the tez jars are put in /apps/ on HDFS, the
        command would be `hadoop dfs -put
        tez-dist/target/tez-0.4.1-incubating/tez-0.4.1-incubating /apps/`
    -   Please do not upload the tarball to HDFS, upload only the jars.
4.  Configure tez-site.xml to set tez.lib.uris to point to the paths in
    HDFS containing the jars. Please note that the paths are not
    searched recursively so for *basedir* and *basedir*/lib/, you will
    need to configure the 2 paths as a comma-separated list. * Assuming
    you followed step 3, the value would be:
    "${fs.default.name}/apps/tez-0.4.1-incubating,${fs.default.name}/apps/tez-0.4.1-incubating/lib/"
5.  Modify mapred-site.xml to change _mapreduce.framework.name_ property
    from its default value of *yarn* to *yarn-tez*
6.  Set HADOOP_CLASSPATH to have the following paths in it:
    -   TEZ_CONF_DIR - location of tez-site.xml
    -   TEZ_JARS and TEZ_JARS/libs - location of the tez jars and
        dependencies.
    -   The command to set up the classpath should be something like:
        `export HADOOP_CLASSPATH=${TEZ_CONF_DIR}:${TEZ_JARS}/*:${TEZ_JARS}/lib/*`
        Please note the "*" which is an important requirement when
        setting up classpaths for directories containing jar files.
7.  Submit a MR job as you normally would using something like:

    ```
    $HADOOP_PREFIX/bin/hadoop jar hadoop-mapreduce-client-jobclient-3.0.0-SNAPSHOT-tests.jar sleep -mt 1 -rt 1 -m 1 -r 1
    ```

    This will use the TEZ DAG ApplicationMaster to run the MR job. This
    can be verified by looking at the AM’s logs from the YARN
    ResourceManager UI.
8.  There is a basic example of using an MRR job in the
    tez-mapreduce-examples.jar. Refer to OrderedWordCount.java in the
    source code. To run this example:

    ``` 
    $HADOOP_PREFIX/bin/hadoop jar tez-mapreduce-examples.jar orderedwordcount <input> <output>
    ```

    This will use the TEZ DAG ApplicationMaster to run the ordered word
    count job. This job is similar to the word count example except that
    it also orders all words based on the frequency of occurrence.

    There are multiple variations to run orderedwordcount. You can use
    it to run multiple DAGs serially on different inputs/outputs. These
    DAGs could be run separately as different applications or serially
    within a single TEZ session.

    ```
    $HADOOP_PREFIX/bin/hadoop jar tez-mapreduce-examples.jar orderedwordcount <input1> <output1> <input2> <output2> <input3> <output3> ...
    ```

    The above will run multiple DAGs for each input-output pair.

    To use TEZ sessions, set -DUSE_TEZ_SESSION=true

    ```
    $HADOOP_PREFIX/bin/hadoop jar tez-mapreduce-examples.jar orderedwordcount -DUSE_TEZ_SESSION=true <input1> <output1> <input2> <output2>
    ```
