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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResource;

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

  /** The staging dir used while submitting DAGs */
  public static final String TEZ_AM_STAGING_DIR = TEZ_PREFIX + "staging-dir";
  public static final String TEZ_AM_STAGING_DIR_DEFAULT = "/tmp/tez/staging";

  public static final String TEZ_APPLICATION_MASTER_CLASS =
      "org.apache.tez.dag.app.DAGAppMaster";

  /** Root Logging level passed to the Tez app master.*/
  public static final String TEZ_AM_LOG_LEVEL = TEZ_AM_PREFIX+"log.level";
  public static final String TEZ_AM_LOG_LEVEL_DEFAULT = "INFO";

  public static final String TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS =
      TEZ_AM_PREFIX + "commit-all-outputs-on-dag-success";
  public static final boolean TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS_DEFAULT = true;

  /** Java options for the Tez AppMaster process. */
  public static final String TEZ_AM_JAVA_OPTS = TEZ_AM_PREFIX
      + "java.opts";
  public static final String TEZ_AM_JAVA_OPTS_DEFAULT = " -Xmx1024m ";

  /** User-provided env for the Tez AM. Any env provided in AMConfiguration
   * overrides env defined by this config property
   * Should be specified as a comma-separated of key-value pairs where each pair
   * is defined as KEY=VAL
   */
  public static final String TEZ_AM_ENV = TEZ_AM_PREFIX + "env";

  public static final String TEZ_AM_CANCEL_DELEGATION_TOKEN = TEZ_AM_PREFIX +
      "am.complete.cancel.delegation.tokens";
  public static final boolean TEZ_AM_CANCEL_DELEGATION_TOKEN_DEFAULT = true;

  public static final String TEZ_AM_TASK_LISTENER_THREAD_COUNT =
      TEZ_AM_PREFIX + "task.listener.thread-count";
  public static final int TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT = 30;

  /*
   * MR AM Service Authorization
   * These are the same as MR which allows Tez to run in secure
   * mode without configuring service ACLs
   */
  public static final String   
  TEZ_AM_SECURITY_SERVICE_AUTHORIZATION_TASK_UMBILICAL =
      "security.job.task.protocol.acl";
  public static final String   
  TEZ_AM_SECURITY_SERVICE_AUTHORIZATION_CLIENT =
      "security.job.client.protocol.acl";

  /**
   * Upper limit on the number of threads user to launch containers in the app
   * master. Expect level config, you shouldn't be needing it in most cases.
   */
  public static final String TEZ_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT =
    TEZ_AM_PREFIX+"containerlauncher.thread-count-limit";

  public static final int TEZ_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT_DEFAULT = 
    500;

  
  // TODO Some of the DAG properties are job specific and not AM specific. Rename accordingly.
  // TODO Are any of these node blacklisting properties required. (other than for MR compat)
  public static final String TEZ_AM_MAX_TASK_FAILURES_PER_NODE = TEZ_AM_PREFIX
      + "maxtaskfailures.per.node";
  public static final int TEZ_AM_MAX_TASK_FAILURES_PER_NODE_DEFAULT = 3;

  public static final String TEZ_AM_MAX_APP_ATTEMPTS = TEZ_AM_PREFIX + 
      "max.app.attempts";
  public static int TEZ_AM_MAX_APP_ATTEMPTS_DEFAULT = 2;
  
  /**
   * The maximum number of attempts that can fail for a particular task. This 
   * does not count killed attempts.
   */
  public static final String TEZ_AM_TASK_MAX_FAILED_ATTEMPTS =
      TEZ_AM_PREFIX + "task.max.failed.attempts";
  public static final int TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT = 4;

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


  /** The amount of memory to be used by the AppMaster */
  public static final String TEZ_AM_RESOURCE_MEMORY_MB = TEZ_AM_PREFIX
      + "resource.memory.mb";
  public static final int TEZ_AM_RESOURCE_MEMORY_MB_DEFAULT = 1536;

  public static final String TEZ_AM_RESOURCE_CPU_VCORES = TEZ_AM_PREFIX
      + "resource.cpu.vcores";
  public static final int TEZ_AM_RESOURCE_CPU_VCORES_DEFAULT = 1;

  public static final String
          TEZ_AM_SLOWSTART_DAG_SCHEDULER_MIN_SHUFFLE_RESOURCE_FRACTION = TEZ_AM_PREFIX
          + "slowstart-dag-scheduler.min-resource-fraction";
  public static final float
          TEZ_AM_SLOWSTART_DAG_SCHEDULER_MIN_SHUFFLE_RESOURCE_FRACTION_DEFAULT = 0.5f;

  /**
   * The complete path to the serialized dag plan file
   * <code>TEZ_AM_PLAN_PB_BINARY</code>. Used to make the plan available to
   * individual tasks if needed. This will typically be a path in the job submit
   * directory.
   */
  public static final String TEZ_AM_PLAN_REMOTE_PATH = TEZ_AM_PREFIX
      + "dag-am-plan.remote.path";

  /** The maximum heartbeat interval between the AM and RM in milliseconds */
  public static final String TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX = TEZ_AM_PREFIX
      + "am-rm.heartbeat.interval-ms.max";
  public static final int TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX_DEFAULT = 1000;

  /** The maximum amount of time, in milliseconds, to wait before a task asks an AM for another task. */
  public static final String TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX = TEZ_TASK_PREFIX
      + "get-task.sleep.interval-ms.max";
  public static final int TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX_DEFAULT = 200;

  public static final String TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS = TEZ_TASK_PREFIX
      + "am.heartbeat.interval-ms.max";
  public static final int TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS_DEFAULT = 100;

  /**
   * Interval after which counters are sent to AM in heartbeat  
   */
  public static final String TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS = TEZ_TASK_PREFIX
      + "am.heartbeat.counter.interval-ms.max";
  public static final int TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS_DEFAULT =
      1000;

  public static final String TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT = TEZ_TASK_PREFIX
      + "max-events-per-heartbeat.max";
  public static final int TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT_DEFAULT = 100;

  /**
   * Whether to generate counters per IO or not. Enabling this will rename
   * CounterGroups / CounterNames to making thme unique per Vertex +
   * Src|Destination
   */
  @Unstable
  @Private
  public static final String TEZ_TASK_GENERATE_COUNTERS_PER_IO = TEZ_TASK_PREFIX
      + "generate.counters.per.io";
  public static final boolean TEZ_TASK_GENERATE_COUNTERS_PER_IO_DEFAULT = false;
  
  public static final String TASK_TIMEOUT = TEZ_TASK_PREFIX + "timeout";

  public static final String TASK_HEARTBEAT_TIMEOUT_MS = TEZ_TASK_PREFIX + "heartbeat.timeout-ms";
  /**
   * Configuration to specify whether container should be reused.
   */
  public static final String TEZ_AM_CONTAINER_REUSE_ENABLED = TEZ_AM_PREFIX
      + "container.reuse.enabled";
  public static final boolean TEZ_AM_CONTAINER_REUSE_ENABLED_DEFAULT = true;

  /**
   * Whether to reuse containers for rack local tasks. Active only if reuse is
   * enabled.
   */
  public static final String TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED =
      TEZ_AM_PREFIX + "container.reuse.rack-fallback.enabled";
  public static final boolean
      TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED_DEFAULT = true;

  /**
   * Whether to reuse containers for non-local tasks. Active only if reuse is
   * enabled.
   */
  public static final String TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED =
      TEZ_AM_PREFIX + "container.reuse.non-local-fallback.enabled";
  public static final boolean
      TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED_DEFAULT = false;

  /**
   * The amount of time to wait before assigning a container to the next level
   * of locality. NODE - RACK - NON_LOCAL
   */
  public static final String
      TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS =
      TEZ_AM_PREFIX + "container.reuse.locality.delay-allocation-millis";
  public static final long
    TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS_DEFAULT = 1000l;

  /**
   * The amount of time to hold on to a container if no task can be assigned to
   * it immediately. Only active when reuse is enabled. Set to -1 to never
   * release a container in a session.
   */
  public static final String TEZ_AM_CONTAINER_SESSION_DELAY_ALLOCATION_MILLIS =
    TEZ_AM_PREFIX + "container.session.delay-allocation-millis";
  public static final long
    TEZ_AM_CONTAINER_SESSION_DELAY_ALLOCATION_MILLIS_DEFAULT = 10000l;

  public static final String TEZ_PB_BINARY_CONF_NAME = "tez-conf.pb";
  public static final String TEZ_PB_PLAN_BINARY_NAME = "tez-dag.pb";
  public static final String TEZ_PB_PLAN_TEXT_NAME = "tez-dag.pb.txt";

  /*
   * Logger properties
   */
  public static final String TEZ_CONTAINER_LOG4J_PROPERTIES_FILE = "tez-container-log4j.properties";
  public static final String TEZ_CONTAINER_LOGGER_NAME = "CLA";
  public static final String TEZ_ROOT_LOGGER_NAME = "tez.root.logger";
  public static final String TEZ_CONTAINER_LOG_FILE_NAME = "syslog";
  public static final String TEZ_CONTAINER_ERR_FILE_NAME = "stderr";
  public static final String TEZ_CONTAINER_OUT_FILE_NAME = "stdout";


  /** The location of the Tez libraries which will be localized for DAGs */
  public static final String TEZ_LIB_URIS = TEZ_PREFIX + "lib.uris";
  /** 
   * Allows to ignore 'tez.lib.uris'. Useful during development as well as 
   * raw Tez application where classpath is propagated with application
   * via {@link LocalResource}s
   */
  public static final String TEZ_IGNORE_LIB_URIS = TEZ_PREFIX + "ignore.lib.uris";

  public static final String TEZ_APPLICATION_TYPE = "TEZ";

  public static final String TEZ_AM_GROUPING_SPLIT_COUNT = TEZ_AM_PREFIX +
      "grouping.split-count";
  public static final String TEZ_AM_GROUPING_SPLIT_BY_LENGTH = TEZ_AM_PREFIX + 
      "grouping.by-length";
  public static final boolean TEZ_AM_GROUPING_SPLIT_BY_LENGTH_DEFAULT = true;
  public static final String TEZ_AM_GROUPING_SPLIT_BY_COUNT = TEZ_AM_PREFIX + 
      "grouping.by-count";
  public static final boolean TEZ_AM_GROUPING_SPLIT_BY_COUNT_DEFAULT = false;
  
  /**
   * The multiplier for available queue capacity when determining number of
   * tasks for a Vertex. 1.7 with 100% queue available implies generating a
   * number of tasks roughly equal to 170% of the available containers on the
   * queue
   */
  public static final String TEZ_AM_GROUPING_SPLIT_WAVES = TEZ_AM_PREFIX +
      "grouping.split-waves";
  public static float TEZ_AM_GROUPING_SPLIT_WAVES_DEFAULT = 1.5f;
  
  /**
   * Upper bound on the size (in bytes) of a grouped split, to avoid generating excessively large splits.
   */
  public static final String TEZ_AM_GROUPING_SPLIT_MAX_SIZE = TEZ_AM_PREFIX +
      "grouping.max-size";
  public static long TEZ_AM_GROUPING_SPLIT_MAX_SIZE_DEFAULT = 
      1024*1024*1024L;

  /**
   * Lower bound on the size (in bytes) of a grouped split, to avoid generating too many splits.
   */
  public static final String TEZ_AM_GROUPING_SPLIT_MIN_SIZE = TEZ_AM_PREFIX +
      "grouping.min-size";
  public static long TEZ_AM_GROUPING_SPLIT_MIN_SIZE_DEFAULT = 
      50*1024*1024L;

  public static final String TEZ_AM_GROUPING_RACK_SPLIT_SIZE_REDUCTION = 
      TEZ_AM_PREFIX + "grouping.rack-split-reduction";
  public static final float TEZ_AM_GROUPING_RACK_SPLIT_SIZE_REDUCTION_DEFAULT = 0.75f;


  /**
   * Session-related properties
   */
  public static final String TEZ_SESSION_PREFIX =
      TEZ_PREFIX + "session.";

  public static final String TEZ_SESSION_LOCAL_RESOURCES_PB_FILE_NAME =
    TEZ_SESSION_PREFIX + "local-resources.pb.file-name";

  /**
   * Time (in seconds) to wait for AM to come up when trying to submit a DAG
   * from the client.
   */
  public static final String TEZ_SESSION_CLIENT_TIMEOUT_SECS =
      TEZ_SESSION_PREFIX + "client.timeout.secs";
  public static final int TEZ_SESSION_CLIENT_TIMEOUT_SECS_DEFAULT =
      120;

  /**
   * Time (in seconds) for which the Tez AM should wait for a DAG to be submitted before
   * shutting down.
   */
  public static final String TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS =
      TEZ_SESSION_PREFIX + "am.dag.submit.timeout.secs";
  public static final int TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS_DEFAULT =
      300;

  /**
   * The queue name for all jobs being submitted as part of a session, or for
   * non session jobs.
   */
  public static final String TEZ_QUEUE_NAME = 
      TEZ_PREFIX + "queue.name";

  public static final String TEZ_GENERATE_DAG_VIZ =
      TEZ_PREFIX + "generate.dag.viz";
  public static final boolean TEZ_GENERATE_DAG_VIZ_DEFAULT = true;

  /**
   * Set of tasks that should be profiled.
   * Format: "vertexName[csv of task ids];vertexName[csv of task ids].."
   * Valid e.g:
   * v[0,1,2]  - Profile tasks 0,1,2 of vertex v
   * v[1,2,3];v2[5,6,7] - Profile specified tasks of vertices v and v2.
   * v[1:5,20,30];v2[2:5,60,7] - Profile 1,2,3,4,5,20,30 of vertex v; 2,3,4,5,60,7 of vertex v2
   * Partial ranges like :5, 1: are not supported.
   * v[] - Profile all tasks in vertex v
   */
  public static final String TEZ_PROFILE_TASK_LIST = TEZ_PREFIX + "profile.task.list";

  /**
   * Additional string to be added to the JVM options for tasks being profiled.
   * __VERTEX_NAME__ and __TASK_INDEX__ can be specified, which would be replaced at
   * runtime by vertex name and task index being profiled.
   * e.g tez.profiler.jvm.opts=--agentpath:libpagent.so,dir=/tmp/__VERTEX_NAME__/__TASK_INDEX__"
   */
  public static final String TEZ_PROFILE_JVM_OPTS = TEZ_PREFIX + "profile.jvm.opts";

  /**
   * The service id for the NodeManager plugin used to share intermediate data
   * between vertices.
   */
  @Private
  public static final String TEZ_SHUFFLE_HANDLER_SERVICE_ID = "mapreduce_shuffle";


  @Private
  public static final String TEZ_PREWARM_DAG_NAME_PREFIX = "TezPreWarmDAG";

  public static final String TEZ_HISTORY_LOGGING_SERVICE_CLASS =
      TEZ_PREFIX + "history.logging.service.class";

  public static final String TEZ_HISTORY_LOGGING_SERVICE_CLASS_DEFAULT =
      "org.apache.tez.dag.history.logging.impl.SimpleHistoryLoggingService";

  public static final String TEZ_SIMPLE_HISTORY_LOGGING_DIR =
      TEZ_PREFIX + "simple.history.logging.dir";
  public static final String TEZ_SIMPLE_HISTORY_LOGGING_MAX_ERRORS =
      TEZ_PREFIX + "simple.history.max.errors";
  public static final int TEZ_SIMPLE_HISTORY_LOGGING_MAX_ERRORS_DEFAULT = 10;

  public static final String YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS =
      TEZ_PREFIX + "yarn.ats.event.flush.timeout.millis";
  public static final long YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS_DEFAULT =
      3000l;

  public static final String DAG_RECOVERY_ENABLED =
      TEZ_PREFIX + "dag.recovery.enabled";
  public static final boolean DAG_RECOVERY_ENABLED_DEFAULT = true;

  public static final String DAG_RECOVERY_FILE_IO_BUFFER_SIZE =
      TEZ_PREFIX + "dag.recovery.io.buffer.size";
  public static final int DAG_RECOVERY_FILE_IO_BUFFER_SIZE_DEFAULT = 8192;

  public static final String DAG_RECOVERY_MAX_UNFLUSHED_EVENTS =
      TEZ_PREFIX + "dag.recovery.max.unflushed.events";
  public static final int DAG_RECOVERY_MAX_UNFLUSHED_EVENTS_DEFAULT = 100;

  public static final String DAG_RECOVERY_FLUSH_INTERVAL_SECS =
      TEZ_PREFIX + "dag.recovery.flush.interval.secs";
  public static final int DAG_RECOVERY_FLUSH_INTERVAL_SECS_DEFAULT = 30;

  public static final String DAG_RECOVERY_DATA_DIR_NAME = "recovery";
  public static final String DAG_RECOVERY_SUMMARY_FILE_SUFFIX = ".summary";
  public static final String DAG_RECOVERY_RECOVER_FILE_SUFFIX = ".recovery";
  
  /**
   *  Tez Local Mode flag. Not valid till Tez-684 get checked-in
   */
  public static final String TEZ_LOCAL_MODE =
    TEZ_PREFIX + "local.mode";

  /**
   *  Tez Local Mode flag. Not valid till Tez-684 get checked-in
   */
  public static final boolean TEZ_LOCAL_MODE_DEFAULT = false;

  /**
   *  Tez AM Inline Mode flag. Not valid till Tez-684 get checked-in
   */
  public static final String TEZ_AM_INLINE_TASK_EXECUTION_ENABLED =
    TEZ_AM_PREFIX + "inline.task.execution.enabled";

  /**
   *  Tez AM Inline Mode flag. Not valid till Tez-684 get checked-in
   */
  public static final boolean TEZ_AM_INLINE_TASK_EXECUTION_ENABLED_DEFAULT = false;

  /**
   * The maximium number of tasks running in parallel in inline mode. Not valid till Tez-684 get checked-in
   */
  public static final String TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS =
    TEZ_AM_PREFIX + "inline.task.execution.max-tasks";

  /**
   * The maximium number of tasks running in parallel in inline mode. Not valid till Tez-684 get checked-in
   */
  public static final int TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS_DEFAULT = 1;
}
