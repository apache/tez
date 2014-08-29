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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.LocalResource;

/**
 * Defines the configurations for Tez. These configurations are typically specified in 
 * tez-site.xml on the client machine where TezClient is used to launch the Tez application.
 * tez-site.xml is expected to be picked up from the classpath of the client process.
 */
@Public
public class TezConfiguration extends Configuration {

  public final static String TEZ_SITE_XML = "tez-site.xml";

  public TezConfiguration() {
    this(true);
  }

  public TezConfiguration(Configuration conf) {
    super(conf);
    addResource(TEZ_SITE_XML);
  }

  public TezConfiguration(boolean loadDefaults) {
    super(loadDefaults);
    if (loadDefaults) {
      addResource(TEZ_SITE_XML);
    }
  }

  @Private
  public static final String TEZ_PREFIX = "tez.";
  @Private
  public static final String TEZ_AM_PREFIX = TEZ_PREFIX + "am.";
  @Private
  public static final String TEZ_TASK_PREFIX = TEZ_PREFIX + "task.";

  /**
   * Boolean value. If true then Tez will try to automatically delete temporary job 
   * artifacts that it creates within the specified staging dir. Does not affect any user data.
   */
  public static final String TEZ_AM_STAGING_SCRATCH_DATA_AUTO_DELETE = TEZ_AM_PREFIX +
      "staging.scratch-data.auto-delete";
  public static final boolean TEZ_AM_STAGING_SCRATCH_DATA_AUTO_DELETE_DEFAULT = true;

  /**
   * String value. Specifies a directory where Tez can create temporary job artifacts.
   */
  public static final String TEZ_AM_STAGING_DIR = TEZ_PREFIX + "staging-dir";
  public static final String TEZ_AM_STAGING_DIR_DEFAULT = "/tmp/tez/staging";
  
  /**
   * String value that is a file path.
   * Path to a credentials file (with serialized credentials) located on the local file system.
   */
  public static final String TEZ_CREDENTIALS_PATH = TEZ_PREFIX + "credentials.path";

  /**
   * Boolean value. Execution mode for the Tez application. True implies session mode. If the client
   * code is written according to best practices then the same code can execute in either mode based
   * on this configuration. Session mode is more aggressive in reserving execution resources and is
   * typically used for interactive applications where multiple DAGs are submitted in quick succession
   * by the same user. For long running applications, one-off executions, batch jobs etc non-session 
   * mode is recommended. If session mode is enabled then container reuse is recommended.
   */
  public static final String TEZ_AM_SESSION_MODE = TEZ_AM_PREFIX + "mode.session";
  public static boolean TEZ_AM_SESSION_MODE_DEFAULT = false;

  /** Root Logging level passed to the Tez app master.*/
  public static final String TEZ_AM_LOG_LEVEL = TEZ_AM_PREFIX + "log.level";
  public static final String TEZ_AM_LOG_LEVEL_DEFAULT = "INFO";

  /** Root Logging level passed to the Tez tasks.*/
  public static final String TEZ_TASK_LOG_LEVEL = TEZ_TASK_PREFIX + "log.level";
  public static final String TEZ_TASK_LOG_LEVEL_DEFAULT = "INFO";

  /**
   * Boolean value. Determines when the final outputs to data sinks are committed. Commit is an
   * output specific operation and typically involves making the output visible for consumption. 
   * If the config is true, then the outputs are committed at the end of DAG completion after all 
   * constituent vertices have completed. If false, outputs for each vertex are committed after that 
   * vertex succeeds. Depending on the desired output visibility and downstream consumer dependencies
   * this value must be appropriately chosen. Defaults to the safe choice of true.
   */
  public static final String TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS =
      TEZ_AM_PREFIX + "commit-all-outputs-on-dag-success";
  public static final boolean TEZ_AM_COMMIT_ALL_OUTPUTS_ON_DAG_SUCCESS_DEFAULT = true;

  /**
   * String value. Command line options provided during the launch of the Tez
   * AppMaster process. Its recommended to not set any Xmx or Xms in these launch opts so that
   * Tez can determine them automatically.
   * */
  public static final String TEZ_AM_LAUNCH_CMD_OPTS = TEZ_AM_PREFIX +  "launch.cmd-opts";
  public static final String TEZ_AM_LAUNCH_CMD_OPTS_DEFAULT = 
      "-server -Djava.net.preferIPv4Stack=true -XX:+PrintGCDetails -verbose:gc " + 
      "-XX:+PrintGCTimeStamps -XX:+UseNUMA -XX:+UseParallelGC " +
      "-Dhadoop.metrics.log.level=WARN ";

  /**
   * String value. Command line options provided during the launch of Tez Task
   * processes. Its recommended to not set any Xmx or Xms in these launch opts
   * so that Tez can determine them automatically.
   */
  public static final String TEZ_TASK_LAUNCH_CMD_OPTS = TEZ_TASK_PREFIX
      + "launch.cmd-opts";
  public static final String TEZ_TASK_LAUNCH_CMD_OPTS_DEFAULT = 
      "-server -Djava.net.preferIPv4Stack=true -XX:+PrintGCDetails -verbose:gc " + 
      "-XX:+PrintGCTimeStamps -XX:+UseNUMA -XX:+UseParallelGC " +
      "-Dhadoop.metrics.log.level=WARN ";

  /**
   * Double value. Tez automatically determines the Xmx for the JVMs used to run
   * Tez tasks and app masters. This feature is enabled if the user has not
   * specified Xmx or Xms values in the launch command opts. Doing automatic Xmx
   * calculation is preferred because Tez can determine the best value based on
   * actual allocation of memory to tasks the cluster. The value if used as a
   * fraction that is applied to the memory allocated Factor to size Xmx based
   * on container memory size. Value should be greater than 0 and less than 1.
   */
  public static final String TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION =
      TEZ_PREFIX + "container.max.java.heap.fraction";
  public static final double TEZ_CONTAINER_MAX_JAVA_HEAP_FRACTION_DEFAULT = 0.8;

  private static final String NATIVE_LIB_PARAM_DEFAULT = Shell.WINDOWS ?
    "PATH=%PATH%;%HADOOP_COMMON_HOME%\\bin":
    "LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_COMMON_HOME/lib/native/";

  /** String value. Env settings for the Tez AppMaster process.
   * Should be specified as a comma-separated of key-value pairs where each pair
   * is defined as KEY=VAL
   * e.g. "LD_LIBRARY_PATH=.,USERNAME=foo"
   * These take least precedence compared to other methods of setting env.
   * These get added to the app master environment prior to launching it.
  */
  public static final String TEZ_AM_LAUNCH_ENV = TEZ_AM_PREFIX
      + "launch.env";
  public static final String TEZ_AM_LAUNCH_ENV_DEFAULT = NATIVE_LIB_PARAM_DEFAULT;

  /** String value. Env settings for the Tez Task processes.
   * Should be specified as a comma-separated of key-value pairs where each pair
   * is defined as KEY=VAL
   * e.g. "LD_LIBRARY_PATH=.,USERNAME=foo"
   * These take least precedence compared to other methods of setting env
   * These get added to the task environment prior to launching it.
   */
  public static final String TEZ_TASK_LAUNCH_ENV = TEZ_TASK_PREFIX
      + "launch.env";
  public static final String TEZ_TASK_LAUNCH_ENV_DEFAULT = NATIVE_LIB_PARAM_DEFAULT;

  @Private
  public static final String TEZ_CANCEL_DELEGATION_TOKENS_ON_COMPLETION = TEZ_PREFIX +
      "cancel.delegation.tokens.on.completion";
  public static final boolean TEZ_CANCEL_DELEGATION_TOKENS_ON_COMPLETION_DEFAULT = true;

  /**
   * Int value. The number of threads used to listen to task heartbeat requests.
   * Expert level setting.
   */
  public static final String TEZ_AM_TASK_LISTENER_THREAD_COUNT =
      TEZ_AM_PREFIX + "task.listener.thread-count";
  public static final int TEZ_AM_TASK_LISTENER_THREAD_COUNT_DEFAULT = 30;

  /**
   * Int value. Configuration to limit the counters per app master. This can be used to
   * limit the amount of memory being used in the app master to store the
   * counters. Expert level setting.
   */
  @Unstable
  public static final String TEZ_AM_COUNTERS_MAX_KEYS = TEZ_AM_PREFIX + "counters.max.keys";
  public static final int TEZ_AM_COUNTERS_MAX_KEYS_DEFAULT = 1200;

  /**
   * Int value. Configuration to limit the counter group names per app master. This can be used to
   * limit the amount of memory being used in the app master to store the
   * counters. Expert level setting.
   */
  @Unstable
  public static final String TEZ_AM_COUNTERS_GROUP_NAME_MAX_KEYS =
      TEZ_AM_PREFIX + "counters.group-name.max.keys";
  public static final int TEZ_AM_COUNTERS_GROUP_NAME_MAX_KEYS_DEFAULT = 128;


  /**
   * Int value. Configuration to limit the counter names per app master. This can be used to
   * limit the amount of memory being used in the app master to store the
   * counters. Expert level setting.
   */
  @Unstable
  public static final String TEZ_AM_COUNTERS_NAME_MAX_KEYS =
      TEZ_AM_PREFIX + "counters.name.max.keys";
  public static final int TEZ_AM_COUNTERS_NAME_MAX_KEYS_DEFAULT = 64;


  /**
   * Int value. Configuration to limit the counter groups per app master. This can be used to
   * limit the amount of memory being used in the app master to store the
   * counters. Expert level setting.
   */
  @Unstable
  public static final String TEZ_AM_COUNTERS_GROUPS_MAX_KEYS =
      TEZ_AM_PREFIX + "counters.groups.max.keys";
  public static final int TEZ_AM_COUNTERS_GROUPS_MAX_KEYS_DEFAULT = 500;

  /**
   * Int value. Upper limit on the number of threads user to launch containers in the app
   * master. Expert level setting. 
   */
  public static final String TEZ_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT =
    TEZ_AM_PREFIX + "containerlauncher.thread-count-limit";

  public static final int TEZ_AM_CONTAINERLAUNCHER_THREAD_COUNT_LIMIT_DEFAULT = 
    500;


  /**
   * Int value. Specifies the number of task failures on a node before the node is considered faulty.
   */
  public static final String TEZ_AM_MAX_TASK_FAILURES_PER_NODE = TEZ_AM_PREFIX
      + "maxtaskfailures.per.node";
  public static final int TEZ_AM_MAX_TASK_FAILURES_PER_NODE_DEFAULT = 3;

  /**
   * Int value. Specifies the number of times the app master can be launched in order to recover 
   * from app master failure. Typically app master failures are non-recoverable. This parameter 
   * is for cases where the app master is not at fault but is lost due to system errors.
   * Expert level setting.
   */
  public static final String TEZ_AM_MAX_APP_ATTEMPTS = TEZ_AM_PREFIX + 
      "max.app.attempts";
  public static int TEZ_AM_MAX_APP_ATTEMPTS_DEFAULT = 2;
  
  /**
   * Int value. The maximum number of attempts that can fail for a particular task before the task is failed. 
   * This does not count killed attempts. Task failure results in DAG failure.
   */
  public static final String TEZ_AM_TASK_MAX_FAILED_ATTEMPTS =
      TEZ_AM_PREFIX + "task.max.failed.attempts";
  public static final int TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT = 4;

  /**
   * Boolean value. Enabled blacklisting of nodes of nodes that are considered faulty. These nodes 
   * will not be used to execute tasks.
   */
  public static final String TEZ_AM_NODE_BLACKLISTING_ENABLED = TEZ_AM_PREFIX
      + "node-blacklisting.enabled";
  public static final boolean TEZ_AM_NODE_BLACKLISTING_ENABLED_DEFAULT = true;
  
  /**
   * Int value. Specifies the percentage of nodes in the cluster that may be considered faulty.
   * This limits the number of nodes that are blacklisted in an effort to minimize the effects of 
   * temporary surges in failures (e.g. due to network outages). 
   */
  public static final String TEZ_AM_NODE_BLACKLISTING_IGNORE_THRESHOLD = TEZ_AM_PREFIX
      + "node-blacklisting.ignore-threshold-node-percent";
  public static final int TEZ_AM_NODE_BLACKLISTING_IGNORE_THRESHOLD_DEFAULT = 33;

  /** Int value. Number of threads to handle client RPC requests. Expert level setting.*/
  public static final String TEZ_AM_CLIENT_THREAD_COUNT =
      TEZ_AM_PREFIX + "client.am.thread-count";
  public static final int TEZ_AM_CLIENT_THREAD_COUNT_DEFAULT = 1;
  
  /**
   * String value. Range of ports that the AM can use when binding for client connections. Leave blank
   * to use all possible ports. Expert level setting.
   */
  public static final String TEZ_AM_CLIENT_AM_PORT_RANGE =
      TEZ_AM_PREFIX + "client.am.port-range";


  /** Int value. The amount of memory in MB to be used by the AppMaster */
  public static final String TEZ_AM_RESOURCE_MEMORY_MB = TEZ_AM_PREFIX
      + "resource.memory.mb";
  public static final int TEZ_AM_RESOURCE_MEMORY_MB_DEFAULT = 1024;

  /** Int value. The number of virtual cores to be used by the app master */
  public static final String TEZ_AM_RESOURCE_CPU_VCORES = TEZ_AM_PREFIX
      + "resource.cpu.vcores";
  public static final int TEZ_AM_RESOURCE_CPU_VCORES_DEFAULT = 1;
  
  /** Int value. The amount of memory in MB to be used by tasks. This applies to all tasks across
   * all vertices. Setting it to the same value for all tasks is helpful for container reuse and 
   * thus good for performance typically. */
  public static final String TEZ_TASK_RESOURCE_MEMORY_MB = TEZ_TASK_PREFIX
      + "resource.memory.mb";
  public static final int TEZ_TASK_RESOURCE_MEMORY_MB_DEFAULT = 1024;

  /**
   * Int value. The number of virtual cores to be used by tasks.
   */
  public static final String TEZ_TASK_RESOURCE_CPU_VCORES = TEZ_TASK_PREFIX
      + "resource.cpu.vcores";
  public static final int TEZ_TASK_RESOURCE_CPU_VCORES_DEFAULT = 1; 

  /**
   * Int value. The maximum heartbeat interval between the AM and RM in milliseconds
   * Increasing this reduces the communication between the AM and the RM and can
   * help in scaling up. Expert level setting. Expert level setting.
   */
  public static final String TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX = TEZ_AM_PREFIX
      + "am-rm.heartbeat.interval-ms.max";
  public static final int TEZ_AM_RM_HEARTBEAT_INTERVAL_MS_MAX_DEFAULT = 1000;

  /**
   * Int value. The maximum amount of time, in milliseconds, to wait before a task asks an
   * AM for another task. Increasing this can help improve app master scalability for a large 
   * number of concurrent tasks. Expert level setting.
   */
  public static final String TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX = TEZ_TASK_PREFIX
      + "get-task.sleep.interval-ms.max";
  public static final int TEZ_TASK_GET_TASK_SLEEP_INTERVAL_MS_MAX_DEFAULT = 200;

  /**
   * Int value. The maximum heartbeat interval, in milliseconds, between the app master and tasks. 
   * Increasing this can help improve app master scalability for a large number of concurrent tasks.
   * Expert level setting.
   */
  public static final String TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS = TEZ_TASK_PREFIX
      + "am.heartbeat.interval-ms.max";
  public static final int TEZ_TASK_AM_HEARTBEAT_INTERVAL_MS_DEFAULT = 100;

  /**
   * Int value. Interval, in milliseconds, after which counters are sent to AM in heartbeat from 
   * tasks. This reduces the amount of network traffice between AM and tasks to send high-volume 
   * counters. Improves AM scalability. Expert level setting.
   */
  public static final String TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS = TEZ_TASK_PREFIX
      + "am.heartbeat.counter.interval-ms.max";
  public static final int TEZ_TASK_AM_HEARTBEAT_COUNTER_INTERVAL_MS_DEFAULT =
      1000;

  /**
   * Int value. Maximum number of of events to fetch from the AM by the tasks in a single heartbeat.
   * Expert level setting. Expert level setting.
   */
  public static final String TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT = TEZ_TASK_PREFIX
      + "max-events-per-heartbeat.max";
  public static final int TEZ_TASK_MAX_EVENTS_PER_HEARTBEAT_DEFAULT = 100;

  /**
   * Whether to generate counters per IO or not. Enabling this will rename
   * CounterGroups / CounterNames to making them unique per Vertex +
   * Src|Destination
   */
  @Unstable
  @Private
  public static final String TEZ_TASK_GENERATE_COUNTERS_PER_IO = TEZ_TASK_PREFIX
      + "generate.counters.per.io";
  @Private
  public static final boolean TEZ_TASK_GENERATE_COUNTERS_PER_IO_DEFAULT = false;

  /**
   * Int value. Time interval, in milliseconds, within which a task must heartbeat to the app master
   * before its considered lost.
   * Expert level setting.
   */
  public static final String TASK_HEARTBEAT_TIMEOUT_MS = TEZ_TASK_PREFIX + "timeout-ms";

  /**
   * Int value. Time interval, in milliseconds, between checks for lost tasks.
   * Expert level setting.
   */
  public static final String TASK_HEARTBEAT_TIMEOUT_CHECK_MS = TEZ_TASK_PREFIX + "heartbeat.timeout.check-ms";
    
  /**
   * Whether to scale down memory requested by each component if the total
   * exceeds the available JVM memory
   */
  @Private
  @Unstable
  public static final String TEZ_TASK_SCALE_TASK_MEMORY_ENABLED = TEZ_TASK_PREFIX
      + "scale.task.memory.enabled";
  @Private
  public static final boolean TEZ_TASK_SCALE_TASK_MEMORY_ENABLED_DEFAULT = true;

  /**
   * The allocator to use for initial memory allocation
   */
  @Private
  @Unstable
  public static final String TEZ_TASK_SCALE_TASK_MEMORY_ALLOCATOR_CLASS = TEZ_TASK_PREFIX
      + "scale.task.memory.allocator.class";
  @Private
  public static final String TEZ_TASK_SCALE_TASK_MEMORY_ALLOCATOR_CLASS_DEFAULT =
      "org.apache.tez.runtime.library.resources.WeightedScalingMemoryDistributor";

  /**
   * The fraction of the JVM memory which will not be considered for allocation.
   * No defaults, since there are pre-existing defaults based on different scenarios.
   */
  @Private
  @Unstable
  public static final String TEZ_TASK_SCALE_TASK_MEMORY_RESERVE_FRACTION = TEZ_TASK_PREFIX
      + "scale.task.memory.reserve-fraction";
  @Private
  public static final double TEZ_TASK_SCALE_TASK_MEMORY_RESERVE_FRACTION_DEFAULT = 0.3d; 

  @Private
  @Unstable
  /**
   * Defines the ProcessTree implementation which will be used to collect resource utilization.
   */
  public static final String TEZ_TASK_RESOURCE_CALCULATOR_PROCESS_TREE_CLASS =
      TEZ_TASK_PREFIX + "resource.calculator.process-tree.class";

  /**
   * Fraction of available memory to reserve per input/output. This amount is
   * removed from the total available pool before allocation and is for factoring in overheads.
   */
  @Private
  @Unstable
  public static final String TEZ_TASK_SCALE_TASK_MEMORY_ADDITIONAL_RESERVATION_FRACTION_PER_IO =
      TEZ_TASK_PREFIX + "scale.task.memory.additional-reservation.fraction.per-io";

  @Private
  @Unstable
  /**
   * Max cumulative total reservation for additional IOs.
   */
  public static final String TEZ_TASK_SCALE_TASK_MEMORY_ADDITIONAL_RESERVATION_FRACTION_MAX =
      TEZ_TASK_PREFIX + "scale.task.memory.additional-reservation.fraction.max";
  /*
   * Weighted ratios for individual component types in the RuntimeLibrary.
   * e.g. PARTITIONED_UNSORTED_OUTPUT:0,UNSORTED_INPUT:1,SORTED_OUTPUT:2,SORTED_MERGED_INPUT:3,PROCESSOR:1,OTHER:1
   */
  @Private
  @Unstable
  public static final String TEZ_TASK_SCALE_TASK_MEMORY_WEIGHTED_RATIOS = 
      TEZ_TASK_PREFIX + "scale.task.memory.ratios";


  /**
   * Boolean value. Configuration to specify whether container should be reused across tasks.
   * This improves performance by not incurring recurring launch overheads.
   */
  public static final String TEZ_AM_CONTAINER_REUSE_ENABLED = TEZ_AM_PREFIX
      + "container.reuse.enabled";
  public static final boolean TEZ_AM_CONTAINER_REUSE_ENABLED_DEFAULT = true;

  /**
   * Boolean value. Whether to reuse containers for rack local tasks. Active only if reuse is
   * enabled.
   */
  public static final String TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED =
      TEZ_AM_PREFIX + "container.reuse.rack-fallback.enabled";
  public static final boolean
      TEZ_AM_CONTAINER_REUSE_RACK_FALLBACK_ENABLED_DEFAULT = true;

  /**
   * Boolean value. Whether to reuse containers for non-local tasks. Active only if reuse is
   * enabled. Turning this on can severely affect locality and can be bad for jobs with high data 
   * volume being read from the primary data sources.
   */
  public static final String TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED =
      TEZ_AM_PREFIX + "container.reuse.non-local-fallback.enabled";
  public static final boolean
      TEZ_AM_CONTAINER_REUSE_NON_LOCAL_FALLBACK_ENABLED_DEFAULT = false;

  /**
   * Int value. The amount of time to wait before assigning a container to the next level
   * of locality. NODE -> RACK -> NON_LOCAL. Delay scheduling parameter. Expert level setting.
   */
  public static final String
      TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS =
      TEZ_AM_PREFIX + "container.reuse.locality.delay-allocation-millis";
  public static final long
    TEZ_AM_CONTAINER_REUSE_LOCALITY_DELAY_ALLOCATION_MILLIS_DEFAULT = 250l;

  /**
   * Int value. The minimum amount of time to hold on to a container that is idle. Only active when 
   * reuse is enabled. Set to -1 to never release idle containers (not recommended). 
   */
  public static final String TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS = 
    TEZ_AM_PREFIX + "container.idle.release-timeout-min.millis";
  public static final long
    TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS_DEFAULT = 5000l;  

  /**
   * Int value. The maximum amount of time to hold on to a container if no task can be
   * assigned to it immediately. Only active when reuse is enabled. The value
   * must be +ve and >=
   * TezConfiguration#TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS.
   * Containers will have an expire time set to a random value between
   * TezConfiguration#TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MIN_MILLIS &&
   * TezConfiguration#TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS. This 
   * creates a graceful reduction in the amount of idle resources held
   */
  public static final String TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS = 
      TEZ_AM_PREFIX + "container.idle.release-timeout-max.millis";
  public static final long
    TEZ_AM_CONTAINER_IDLE_RELEASE_TIMEOUT_MAX_MILLIS_DEFAULT = 10000l;
  
  /**
   * Int value. The minimum number of containers that will be held in session mode. Not active in 
   * non-session mode. Enables an idle session (not running any DAG) to hold on to a minimum number
   * of containers to provide fast response times for the next DAG.
   */
  public static final String TEZ_AM_SESSION_MIN_HELD_CONTAINERS = 
      TEZ_AM_PREFIX + "session.min.held-containers";
  public static final int TEZ_AM_SESSION_MIN_HELD_CONTAINERS_DEFAULT = 0;

  /**
   * String value to a file path.
   * The location of the Tez libraries which will be localized for DAGs.
   * This follows the following semantics
   * <ol>
   * <li> To use a single .tar.gz or .tgz file (generated by the tez build), the full path to this
   * file (including filename) should be specified. The internal structure of the uncompressed tgz
   * will be retained under $CWD/tezlib.</li>
   *
   * <li> If a single file is specified without the above mentioned extensions - it will be treated as
   * a regular file. This means it will not be uncompressed during runtime. </li>
   *
   * <li> If multiple entries exist
   * <ul>
   * <li> Files: will be treated as regular files (not uncompressed during runtime) </li>
   * <li> Directories: all files under the directory (non-recursive) will be made available (but not
   * uncompressed during runtime). </li>
   * <li> All files / contents of directories are flattened into a single directory - $CWD </li>
   * </ul>
   * </ol>
   */
  public static final String TEZ_LIB_URIS = TEZ_PREFIX + "lib.uris";
  
  /** 
   * Boolean value. Allows to ignore 'tez.lib.uris'. Useful during development as well as 
   * raw Tez application where classpath is propagated with application
   * via {@link LocalResource}s. This is mainly useful for developer/debugger scenarios.
   */
  public static final String TEZ_IGNORE_LIB_URIS = TEZ_PREFIX + "ignore.lib.uris";

  /**
   * Boolean value.
   * Specify whether hadoop libraries required to run Tez should be the ones deployed on the cluster.
   * This is disabled by default - with the expectation being that tez.lib.uris has a complete
   * tez-deployment which contains the hadoop libraries.
   */
  public static final String TEZ_USE_CLUSTER_HADOOP_LIBS = TEZ_PREFIX + "use.cluster.hadoop-libs";
  public static final boolean TEZ_USE_CLUSTER_HADOOP_LIBS_DEFAULT = false;

  /**
   * Session-related properties
   */
  @Private
  public static final String TEZ_SESSION_PREFIX =
      TEZ_PREFIX + "session.";

  /**
   * Int value. Time (in seconds) to wait for AM to come up when trying to submit a DAG
   * from the client. Only relevant in session mode. If the cluster is busy and cannot launch the 
   * AM then this timeout may be hit. In those case, using non-session mode is recommended if 
   * applicable. Otherwise increase the timeout (set to -1 for infinity. Not recommended)
   */
  public static final String TEZ_SESSION_CLIENT_TIMEOUT_SECS =
      TEZ_SESSION_PREFIX + "client.timeout.secs";
  public static final int TEZ_SESSION_CLIENT_TIMEOUT_SECS_DEFAULT =
      120;

  /**
   * Int value. Time (in seconds) for which the Tez AM should wait for a DAG to be submitted before
   * shutting down. Only relevant in session mode.
   */
  public static final String TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS =
      TEZ_SESSION_PREFIX + "am.dag.submit.timeout.secs";
  public static final int TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS_DEFAULT =
      300;

  /**
   * String value. The queue name for all jobs being submitted from a given client.
   */
  public static final String TEZ_QUEUE_NAME = TEZ_PREFIX + "queue.name";

  @Unstable
  /**
   * Boolean value. Generate debug artifacts like DAG visualization.
   */
  public static final String TEZ_GENERATE_DEBUG_ARTIFACTS =
      TEZ_PREFIX + "generate.debug.artifacts";
  public static final boolean TEZ_GENERATE_DEBUG_ARTIFACTS_DEFAULT = true;

  /**
   * Set of tasks for which specific launch command options need to be added.
   * Format: "vertexName[csv of task ids];vertexName[csv of task ids].."
   * Valid e.g:
   * v[0,1,2]  - Additional launch-cmd options for tasks 0,1,2 of vertex v
   * v[1,2,3];v2[5,6,7] - Additional launch-cmd options specified for tasks of vertices v and v2.
   * v[1:5,20,30];v2[2:5,60,7] - Additional launch-cmd options for 1,2,3,4,5,20,30 of vertex v; 2,
   * 3,4,5,60,7 of vertex v2
   * Partial ranges like :5, 1: are not supported.
   * v[] - Additional launch-cmd options for all tasks in vertex v
   */
  @Unstable
  public static final String TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS_LIST = TEZ_PREFIX + "task-specific" +
      ".launch.cmd-opts.list";

  /**
   * Additional launch command options to be added for specific tasks.
   * __VERTEX_NAME__ and __TASK_INDEX__ can be specified, which would be replaced at
   * runtime by vertex name and task index.
   * e.g tez.task-specific.launch.cmd-opts=
   * "-agentpath:libpagent.so,dir=/tmp/__VERTEX_NAME__/__TASK_INDEX__"
   */
  @Unstable
  public static final String TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS = TEZ_PREFIX + "task-specific" +
      ".launch.cmd-opts";

  /**
   * String value that is a class name.
   * Specify the class to use for logging history data
   */
  public static final String TEZ_HISTORY_LOGGING_SERVICE_CLASS =
      TEZ_PREFIX + "history.logging.service.class";

  public static final String TEZ_HISTORY_LOGGING_SERVICE_CLASS_DEFAULT =
      "org.apache.tez.dag.history.logging.impl.SimpleHistoryLoggingService";

  /**
   * String value. The directory into which history data will be written. This defaults to the 
   * container logging directory. This is relevant only when SimpleHistoryLoggingService is being
   * used for {@link TezConfiguration#TEZ_HISTORY_LOGGING_SERVICE_CLASS}
   */
  public static final String TEZ_SIMPLE_HISTORY_LOGGING_DIR =
      TEZ_PREFIX + "simple.history.logging.dir";
  
  /**
   * Int value. Maximum errors allowed while logging history data. After crossing this limit history
   * logging gets disabled. The job continues to run after this.
   */
  public static final String TEZ_SIMPLE_HISTORY_LOGGING_MAX_ERRORS =
      TEZ_PREFIX + "simple.history.max.errors";
  public static final int TEZ_SIMPLE_HISTORY_LOGGING_MAX_ERRORS_DEFAULT = 10;

  /**
   * Int value. Time, in milliseconds, to wait while flushing YARN ATS data during shutdown.
   * Expert level setting.
   */
  public static final String YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS =
      TEZ_PREFIX + "yarn.ats.event.flush.timeout.millis";
  public static final long YARN_ATS_EVENT_FLUSH_TIMEOUT_MILLIS_DEFAULT =
      3000l;

  /**
   * Boolean value. Enable recovery of DAGs. This allows a restarted app master to recover the 
   * incomplete DAGs from the previous instance of the app master.
   */
  public static final String DAG_RECOVERY_ENABLED =
      TEZ_PREFIX + "dag.recovery.enabled";
  public static final boolean DAG_RECOVERY_ENABLED_DEFAULT = true;

  /**
   * Int value. Size in bytes for the IO buffer size while processing the recovery file.
   * Expert level setting.
   */
  public static final String DAG_RECOVERY_FILE_IO_BUFFER_SIZE =
      TEZ_PREFIX + "dag.recovery.io.buffer.size";
  public static final int DAG_RECOVERY_FILE_IO_BUFFER_SIZE_DEFAULT = 8192;

  /**
   * Int value. Number of recovery events to buffer before flushing them to the recovery log.
   */
  public static final String DAG_RECOVERY_MAX_UNFLUSHED_EVENTS =
      TEZ_PREFIX + "dag.recovery.max.unflushed.events";
  public static final int DAG_RECOVERY_MAX_UNFLUSHED_EVENTS_DEFAULT = 100;

  /**
   * Int value. Interval, in seconds, between flushing recovery data to the recovery log.
   */
  public static final String DAG_RECOVERY_FLUSH_INTERVAL_SECS =
      TEZ_PREFIX + "dag.recovery.flush.interval.secs";
  public static final int DAG_RECOVERY_FLUSH_INTERVAL_SECS_DEFAULT = 30;

  /**
   *  Boolean value. Enable local mode execution in Tez. Enables tasks to run in the same process as
   *  the app master. Primarily used for debugging.
   */
  public static final String TEZ_LOCAL_MODE =
    TEZ_PREFIX + "local.mode";

  public static final boolean TEZ_LOCAL_MODE_DEFAULT = false;

  /**
   *  Tez AM Inline Mode flag. Not valid till Tez-684 get checked-in
   */
  @Private
  public static final String TEZ_AM_INLINE_TASK_EXECUTION_ENABLED =
    TEZ_AM_PREFIX + "inline.task.execution.enabled";

  /**
   *  Tez AM Inline Mode flag. Not valid till Tez-684 get checked-in
   */
  @Private
  public static final boolean TEZ_AM_INLINE_TASK_EXECUTION_ENABLED_DEFAULT = false;

  /**
   * Int value.
   * The maximium number of tasks running in parallel within the app master process.
   */
  public static final String TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS =
    TEZ_AM_PREFIX + "inline.task.execution.max-tasks";

  public static final int TEZ_AM_INLINE_TASK_EXECUTION_MAX_TASKS_DEFAULT = 1;


  // ACLs related configuration
  // Format supports a comma-separated list of users and groups with the users and groups separated
  // by whitespace. e.g. "user1,user2 group1,group2"
  // All users/groups that have access to do operations on the AM also have access to similar
  // operations on all DAGs within that AM/session.
  // By default, the "owner" i.e. the user who started the session will always have full admin
  // access to the AM. Also, the user that submitted the DAG has full admin access to all operations
  // on that DAG.
  //
  // If no value is specified or an invalid configuration is specified,
  // only the user who submitted the AM and/or DAG can do the appropriate operations.
  // For example, "user1,user2 group1, group2" is an invalid configuration value as splitting by
  // whitespace produces 3 lists instead of 2.

  // If the value specified is "*", all users are allowed to do the operation.

  /**
   * Boolean value. Configuration to enable/disable ACL checks.
   */
  public static final String TEZ_AM_ACLS_ENABLED = TEZ_AM_PREFIX + "acls.enabled";
  public static final boolean TEZ_AM_ACLS_ENABLED_DEFAULT = true;

  /**
   * String value. 
   * AM view ACLs. This allows the specified users/groups to view the status of the AM and all DAGs
   * that run within this AM.
   * Comma separated list of users, followed by whitespace, followed by a comma separated list of 
   * groups
   */
  public static final String TEZ_AM_VIEW_ACLS = TEZ_AM_PREFIX + "view-acls";

  /**
   * String value.
   * AM modify ACLs. This allows the specified users/groups to run modify operations on the AM
   * such as submitting DAGs, pre-warming the session, killing DAGs or shutting down the session.
   * Comma separated list of users, followed by whitespace, followed by a comma separated list of 
   * groups
   */
  public static final String TEZ_AM_MODIFY_ACLS = TEZ_AM_PREFIX + "modify-acls";

}
