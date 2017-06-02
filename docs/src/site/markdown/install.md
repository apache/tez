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
Replace x.y.z with the tez release number that you are using. E.g. 0.5.0. For Tez 
versions 0.8.3 and higher, Tez needs Apache Hadoop to be of version 2.6.0 or higher.
For Tez version 0.9.0 and higher, Tez needs Apache Hadoop to be version 2.7.0
or higher.

1.  Deploy Apache Hadoop using version of 2.7.0 or higher.
    -   You need to change the value of the hadoop.version property in the
        top-level pom.xml to match the version of the hadoop branch being used.

    ```
    $ hadoop version
    ```

2.  Build tez using `mvn clean package -DskipTests=true -Dmaven.javadoc.skip=true`
    -   This assumes that you have already installed JDK8 or later and Maven 3 or later.
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
    hadoop fs -mkdir /apps/tez-x.y.z-SNAPSHOT
    hadoop fs -copyFromLocal tez-dist/target/tez-x.y.z-SNAPSHOT.tar.gz /apps/tez-x.y.z-SNAPSHOT/
    ```

    -   tez-site.xml configuration.
        -   Set tez.lib.uris to point to the tar.gz uploaded to HDFS.
            Assuming the steps mentioned so far were followed,
            set tez.lib.uris to `${fs.defaultFS}/apps/tez-x.y.z-SNAPSHOT/tez-x.y.z-SNAPSHOT.tar.gz`
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

Various ways to configure tez.lib.uris
---------------------------------------

The `tez.lib.uris` configuration property supports a comma-separated list of values. The 
types of values supported are:
  - Path to simple file
  - Path to a directory
  - Path to a compressed archive ( tarball, zip, etc).

For simple files and directories, Tez will add all these files and first-level entries in the
directories (recursive traversal of dirs is not supported) into the working directory of the
Tez runtime and they will automatically be included into the classpath. For archives i.e.
files whose names end with generally known compressed archive suffixes such as 'tgz',
'tar.gz', 'zip', etc. will be uncompressed into the container working directory too. However,
given that the archive structure is not known to the Tez framework, the user is expected to
configure `tez.lib.uris.classpath` to ensure that the nested directory structure of an
archive is added to the classpath. This classpath values should be relative i.e. the entries
should start with "./".

Hadoop Installation dependent Install/Deploy Instructions
---------------------------------------------------------

The above install instructions use Tez with pre-packaged Hadoop libraries included in the package and is the
recommended method for installation. A full tarball with all dependencies is a better approach to ensure
that existing jobs continue to run during a cluster's rolling upgrade.

Although the `tez.lib.uris` configuration options enable a wide variety of usage patterns, there
are 2 main alternative modes that are supported by the framework: 

1. Mode A: Using a tez tarball on HDFS along with Hadoop libraries available on the cluster.
2. Mode B: Using a tez tarball along with the Hadoop tarball.
 
Both these modes will require a tez build without Hadoop dependencies and that is available at
tez-dist/target/tez-x.y.z-minimal.tar.gz.

For Mode A: Tez tarball with using existing cluster Hadoop libraries by leveraging yarn.application.classpath
-------------------------------------------------------------------------------------------------------------

This mode is not recommended for clusters that use rolling upgrades. Additionally, it is the user's responsibility
to ensure that the tez version being used is compatible with the version of Hadoop running on the cluster.
Step 3 above changes as follows. Also subsequent steps should use tez-dist/target/tez-x.y.z-minimal.tar.gz
instead of tez-dist/target/tez-x.y.z.tar.gz

  - A tez build without Hadoop dependencies will be available at tez-dist/target/tez-x.y.z-minimal.tar.gz
    Assuming that the tez jars are put in /apps/ on HDFS, the command would be

    ```
    "hadoop fs -mkdir /apps/tez-x.y.z"
    "hadoop fs -copyFromLocal tez-dist/target/tez-x.y.z-minimal.tar.gz /apps/tez-x.y.z"
    ```

  - tez-site.xml configuration
    - Set tez.lib.uris to point to the paths in HDFS containing the tez jars. Assuming the steps mentioned so far were followed,
set tez.lib.uris to `${fs.defaultFS}/apps/tez-x.y.z/tez-x.y.z-minimal.tar.gz`
    - Set tez.use.cluster.hadoop-libs to true

For Mode B: Tez tarball with Hadoop tarball
--------------------------------------------

This mode will support rolling upgrades. It is the user's responsibility to ensure that the
versions of Tez and Hadoop being used are compatible.
To do this configuration, we need to change Step 3 of the
default instructions in the following ways.

  - Assuming that the tez archives/jars are put in /apps/ on HDFS, the command to put this
minimal Tez archive into HDFS would be:

  ```
  "hadoop fs -mkdir /apps/tez-x.y.z"
  "hadoop fs -copyFromLocal tez-dist/target/tez-x.y.z-minimal.tar.gz /apps/tez-x.y.z"
  ```

  - Alternatively, you can put the minimal directory directly into HDFS and
  reference the jars, instead of using an archive. The command to put
  the minimal directory into HDFS would be:

  ```
  "hadoop fs -copyFromLocal tez-dist/target/tez-x.y.z-minimal/* /apps/tez-x.y.z"
  ```

  - After building hadoop, the hadoop tarball will be available at
  hadoop/hadoop-dist/target/hadoop-x.y.z-SNAPSHOT.tar.gz
  - Assuming that the hadoop jars are put in /apps/ on HDFS, the command to put this
    Hadoop archive into HDFS would be:

  ```
  "hadoop fs -mkdir /apps/hadoop-x.y.z"
  "hadoop fs -copyFromLocal hadoop-dist/target/hadoop-x.y.z-SNAPSHOT.tar.gz /apps/hadoop-x.y.z"
  ```

  - tez-site.xml configuration
     - Set tez.lib.uris to point to the the archives and jars that are needed for Tez/Hadoop.

     - Example: When using both Tez and Hadoop archives, set tez.lib.uris to
     `${fs.defaultFS}/apps/tez-x.y.z/tez-x.y.z-minimal.tar.gz#tez,${fs.defaultFS}/apps/hadoop-x.y.z/hadoop-x.y.z-SNAPSHOT.tar.gz#hadoop-mapreduce`

    - Example: When using Tez jars with a Hadoop archive, set tez.lib.uris to:
    `${fs.defaultFS}/apps/tez-x.y.z,${fs.defaultFS}/apps/tez-x.y.z/lib,${fs.defaultFS}/apps/hadoop-x.y.z/hadoop-x.y.z-SNAPSHOT.tar.gz#hadoop-mapreduce`

    - In tez.lib.uris, the text immediately following the '#' symbol is the fragment that
      refers to the symlink that will be created for the archive.  If no fragment is given,
      the symlink will be set to the name of the archive. Fragments should not be given
      to directories or jars.

    - If any archives are specified in tez.lib.uris, then tez.lib.uris.classpath must be set
      to define the classpath for these archives as the archive structure is not known. 
    - Example: Classpath when using both Tez and Hadoop archives, set tez.lib.uris.classpath to:

    ```
./tez/*:./tez/lib/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/common/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/common/lib/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/hdfs/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/hdfs/lib/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/yarn/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/yarn/lib/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/mapreduce/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/mapreduce/lib/*
    ```

    - Example: Classpath when using Tez jars with a Hadoop archive, set tez.lib.uris.classpath to:

    ```
./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/common/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/common/lib/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/hdfs/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/hdfs/lib/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/yarn/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/yarn/lib/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/mapreduce/*:./hadoop-mapreduce/hadoop-x.y.z-SNAPSHOT/share/hadoop/mapreduce/lib/*
    ```


[Install instructions for older versions of Tez (pre 0.5.0)](./install_pre_0_5_0.html)
-----------------------------------------------------------------------------------


