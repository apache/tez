/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapred;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.engine.records.TezVertexID;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

public class MapReduceChildJVM2 {

  private static String getTaskLogFile(LogName filter) {
    return ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + 
        filter.toString();
  }
  
  public static void setVMEnv(Map<String, String> environment, JobConf conf,
      TezVertexID vertexId) {

    // FIXME this should be derivable from the container id set by the NM
    // and not require the AM to set
    environment.put(MRJobConfig.APPLICATION_ATTEMPT_ID_ENV,
        conf.get(MRJobConfig.APPLICATION_ATTEMPT_ID).toString());

  }

  public static List<String> getVMCommand(
      InetSocketAddress taskAttemptListenerAddr, JobConf conf, 
      TezVertexID vertexId, 
      ContainerId containerId, ApplicationId jobID, boolean shouldProfile) {

    Vector<String> vargs = new Vector<String>(9);

    vargs.add("exec");
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    // Add child (task) java-vm options.
    // FIXME add support for child java opts

    Path childTmpDir = new Path(Environment.PWD.$(),
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add("-Djava.io.tmpdir=" + childTmpDir);

    // FIXME Setup the log4j properties

    // Decision to profile needs to be made in the scheduler.
    if (shouldProfile) {
      // FIXME add support for profiling
    }

    // Add main class and its arguments 
    vargs.add(YarnTezDagChild.class.getName());  // main of Child
    // pass TaskAttemptListener's address
    vargs.add(taskAttemptListenerAddr.getAddress().getHostAddress()); 
    vargs.add(Integer.toString(taskAttemptListenerAddr.getPort()));
    // Set the job id
    vargs.add(jobID.toString());

    // Finally add the containerId.
    vargs.add(String.valueOf(containerId.toString()));
    vargs.add("1>" + getTaskLogFile(TaskLog.LogName.STDOUT));
    vargs.add("2>" + getTaskLogFile(TaskLog.LogName.STDERR));

    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    Vector<String> vargsFinal = new Vector<String>(1);
    vargsFinal.add(mergedCommand.toString());
    return vargsFinal;
  }

}
