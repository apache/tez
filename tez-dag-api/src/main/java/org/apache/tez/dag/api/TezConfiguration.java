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

package org.apache.tez.dag.api;

import org.apache.hadoop.conf.Configuration;

public class TezConfiguration extends Configuration {

  public final static String TEZ_SITE_XML = "tez-site.xml";

  static {
    addDefaultResource(TEZ_SITE_XML);
  }

  public TezConfiguration() {
    super();
  }

  public TezConfiguration(Configuration conf) {
    super(conf);
  }

  public static final String TEZ_PREFIX = "tez.";
  public static final String DAG_AM_PREFIX = TEZ_PREFIX + "dag.am.";
  
  public static final String DAG_AM_STAGING_DIR = TEZ_PREFIX + "staging-dir";
  public static final String DAG_AM_STAGING_DIR_DEFAULT = "/tmp/hadoop-yarn/staging";
  
  // TODO Should not be required once all tokens are handled via AppSubmissionContext
  public static final String JOB_SUBMIT_DIR = TEZ_PREFIX + "jobSubmitDir";
  public static final String APPLICATION_TOKENS_FILE = "appTokens";
  
  public static final String DAG_AM_TASK_LISTENER_THREAD_COUNT = 
                                TEZ_PREFIX + "task.listener.thread-count";
  public static final int DAG_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT = 30;

  public static final String DAG_AM_CONTAINER_LISTENER_THREAD_COUNT = 
      TEZ_PREFIX + "container.listener.thread-count";
  public static final int DAG_AM_CONTAINER_LISTENER_THREAD_COUNT_DEFAULT = 30;
  
  // TODO Some of the DAG properties are job specific and not AM specific. Rename accordingly.
  // TODO Are any of these node blacklisting properties required. (other than for MR compat)
  public static final String DAG_MAX_TASK_FAILURES_PER_NODE = TEZ_PREFIX
      + "maxtaskfailures.per.node";
  public static final int DAG_MAX_TASK_FAILURES_PER_NODE_DEFAULT = 3;
  public static final String DAG_NODE_BLACKLISTING_ENABLED = TEZ_PREFIX
      + "node-blacklisting.enabled";
  public static final boolean DAG_NODE_BLACKLISTING_ENABLED_DEFAULT = true;
  public static final String DAG_NODE_BLACKLISTING_IGNORE_THRESHOLD = TEZ_PREFIX
      + "node-blacklisting.ignore-threshold-node-percent";
  public static final int DAG_NODE_BLACKLISTING_IGNORE_THRESHOLD_DEFAULT = 33;


  public static final String DAG_AM_RESOURCE_MEMORY_MB = DAG_AM_PREFIX
      + "resource.memory.mb";
  public static final int DEFAULT_DAG_AM_RESOURCE_MEMORY_MB = 1024;

  public static final String DAG_AM_RESOURCE_CPU_VCORES = DAG_AM_PREFIX
      + "resource.cpu.vcores";
  public static final int DEFAULT_DAG_AM_RESOURCE_CPU_VCORES = 1;
  
  public static final String 
          SLOWSTART_VERTEX_SCHEDULER_MIN_SRC_FRACTION = TEZ_PREFIX
          + "slowstart-vertex-scheduler.min-src-fraction";
  public static final float 
          SLOWSTART_VERTEX_SCHEDULER_MIN_SRC_FRACTION_DEFAULT = 0.5f;

  public static final String 
          SLOWSTART_VERTEX_SCHEDULER_MAX_SRC_FRACTION = TEZ_PREFIX
          + "slowstart-vertex-scheduler.max-src-fraction";
  public static final float 
          SLOWSTART_VERTEX_SCHEDULER_MAX_SRC_FRACTION_DEFAULT = 0.8f;

private static final String TEZ_CONF_DIR_ENV = "TEZ_CONF_DIR";
  private static final String TEZ_HOME_ENV = "TEZ_HOME";

  public static final String TEZ_APPLICATION_CLASSPATH = TEZ_PREFIX
      + "application.classpath";
  public static final String[] DEFAULT_TEZ_APPLICATION_CLASSPATH = {
    TEZ_CONF_DIR_ENV,
    TEZ_HOME_ENV + "/*",
    TEZ_HOME_ENV + "/lib/*"
  };

  public static final String DAG_AM_PLAN_CONFIG_XML = "tez-dag.xml";
}
