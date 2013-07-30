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
  public static final String TEZ_AM_PREFIX = TEZ_PREFIX + "am.";
  public static final String TEZ_TASK_PREFIX = TEZ_PREFIX + "task.";

  public static final String TEZ_AM_STAGING_DIR = TEZ_PREFIX + "staging-dir";
  public static final String TEZ_AM_STAGING_DIR_DEFAULT = "/tmp/hadoop-yarn/staging";

  // TODO Should not be required once all tokens are handled via AppSubmissionContext
  public static final String JOB_SUBMIT_DIR = TEZ_PREFIX + "jobSubmitDir";
  public static final String APPLICATION_TOKENS_FILE = "appTokens";
  public static final String TEZ_APPLICATION_MASTER_CLASS =
      "org.apache.tez.dag.app.DAGAppMaster";

  /** Root Logging level passed to the Tez app master.*/
  public static final String TEZ_AM_LOG_LEVEL = TEZ_AM_PREFIX+"log.level";
  public static final String TEZ_AM_LOG_LEVEL_DEFAULT = "INFO";

  public static final String TEZ_AM_CANCEL_DELEGATION_TOKEN = TEZ_AM_PREFIX +
      "am.complete.cancel.delegation.tokens";
  public static final boolean TEZ_AM_CANCEL_DELEGATION_TOKEN_DEFAULT = true;

  public static final String TEZ_AM_TASK_LISTENER_THREAD_COUNT =
      TEZ_AM_PREFIX + "task.listener.thread-count";
  public static final int TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT = 30;

  public static final String TEZ_AM_CONTAINER_LISTENER_THREAD_COUNT =
      TEZ_AM_PREFIX + "container.listener.thread-count";
  public static final int TEZ_AM_CONTAINER_LISTENER_THREAD_COUNT_DEFAULT = 30;

  // TODO Some of the DAG properties are job specific and not AM specific. Rename accordingly.
  // TODO Are any of these node blacklisting properties required. (other than for MR compat)
  public static final String TEZ_AM_MAX_TASK_FAILURES_PER_NODE = TEZ_AM_PREFIX
      + "maxtaskfailures.per.node";
  public static final int TEZ_AM_MAX_TASK_FAILURES_PER_NODE_DEFAULT = 3;

  public static final String TEZ_AM_MAX_TASK_ATTEMPTS =
      TEZ_AM_PREFIX + "max.task.attempts";
  public static final int TEZ_AM_MAX_TASK_ATTEMPTS_DEFAULT = 4;

  public static final String TEZ_AM_NODE_BLACKLISTING_ENABLED = TEZ_AM_PREFIX
      + "node-blacklisting.enabled";
  public static final boolean TEZ_AM_NODE_BLACKLISTING_ENABLED_DEFAULT = true;
  public static final String TEZ_AM_NODE_BLACKLISTING_IGNORE_THRESHOLD = TEZ_AM_PREFIX
      + "node-blacklisting.ignore-threshold-node-percent";
  public static final int TEZ_AM_NODE_BLACKLISTING_IGNORE_THRESHOLD_DEFAULT = 33;

  /** Number of threads to handle job client RPC requests.*/
  public static final String TEZ_AM_CLIENT_THREAD_COUNT =
      TEZ_AM_PREFIX + "client.am.thread-count";
  public static final int TEZ_AM_CLIENT_THREAD_COUNT_DEFAULT = 1;
  /**
   * Range of ports that the AM can use when binding. Leave blank
   * if you want all possible ports.
   */
  public static final String TEZ_AM_CLIENT_AM_PORT_RANGE =
      TEZ_AM_PREFIX + "client.am.port-range";


  public static final String TEZ_AM_RESOURCE_MEMORY_MB = TEZ_AM_PREFIX
      + "resource.memory.mb";
  public static final int TEZ_AM_RESOURCE_MEMORY_MB_DEFAULT = 1536;

  public static final String TEZ_AM_RESOURCE_CPU_VCORES = TEZ_AM_PREFIX
      + "resource.cpu.vcores";
  public static final int TEZ_AM_RESOURCE_CPU_VCORES_DEFAULT = 1;

  public static final String
          TEZ_AM_SLOWSTART_VERTEX_SCHEDULER_MIN_SRC_FRACTION = TEZ_AM_PREFIX
          + "slowstart-vertex-scheduler.min-src-fraction";
  public static final float
          TEZ_AM_SLOWSTART_VERTEX_SCHEDULER_MIN_SRC_FRACTION_DEFAULT = 0.25f;

  public static final String
          TEZ_AM_SLOWSTART_VERTEX_SCHEDULER_MAX_SRC_FRACTION = TEZ_AM_PREFIX
          + "slowstart-vertex-scheduler.max-src-fraction";
  public static final float
          TEZ_AM_SLOWSTART_VERTEX_SCHEDULER_MAX_SRC_FRACTION_DEFAULT = 0.75f;
  
  public static final String 
          TEZ_AM_SLOWSTART_DAG_SCHEDULER_MIN_SHUFFLE_RESOURCE_FRACTION = TEZ_AM_PREFIX
          + "slowstart-dag-scheduler.min-resource-fraction";
  public static final float 
          TEZ_AM_SLOWSTART_DAG_SCHEDULER_MIN_SHUFFLE_RESOURCE_FRACTION_DEFAULT = 0.5f;
  
  public static final String TEZ_AM_AGGRESSIVE_SCHEDULING = TEZ_AM_PREFIX +
      "aggressive.scheduling";
  public static boolean TEZ_AM_AGGRESSIVE_SCHEDULING_DEFAULT = false;

  /**
   * The complete path to the serialized dag plan file
   * <code>TEZ_AM_PLAN_PB_BINARY</code>. Used to make the plan available to
   * individual tasks if needed. This will typically be a path in the job submit
   * directory.
   */
  public static final String TEZ_AM_PLAN_REMOTE_PATH = TEZ_AM_PREFIX
      + "dag-am-plan.remote.path";
  
  public static final String TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX = TEZ_AM_PREFIX
      + "am-rm.heartbeat.interval-ms.max";
  public static final int TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX_DEFAULT = 1000;
  
  public static final String TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX = TEZ_TASK_PREFIX
      + "get-task.sleep.interval-ms.max";
  public static final int TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX_DEFAULT = 500;
  

  public static final String TEZ_AM_PLAN_PB_BINARY = "tez-dag.pb";
  public static final String TEZ_AM_PLAN_PB_TEXT = "tez-dag.pb.txt";

  public static final String TEZ_LIB_URIS =
      TEZ_PREFIX + "lib.uris";

  public static final String TEZ_APPLICATION_TYPE = "TEZ-MR*";
  
  public static final String LOCAL_FRAMEWORK_NAME = "local-tez";
}
