These utils helps to run MapReduce jobs in 'local mode' (i.e. using LocalJobRunner) using TEZ runtime instead of the MapReduce runtime.

Steps:
*   Apply MR_LJR_revoke.patch to remove LocalJobRunner in Apache Hadoop MapReduce project.
*   Run 'mvn install -DskipTests=true' to install the modified Apache Hadoop MapReduce jars in your local maven repo.
*   Setup the Apache Hadoop MapReduce project to pick up TEZ runtime by following:  
   - Run 'mvn package' to create TEZ jars
   - Set TEZ_SRC_DIR to the base of your TEZ source tree.
   - Set HADOOP_MAPRED_HOME to the base of the hadoop-3.0.0-SNAPSHOT directory
   - Link required TEZ jars into Apache Hadoop MapReduce lib directory using ln_tez_jar.sh
*   That's it! Run jobs in local-mode using TEZ!
