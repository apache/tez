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

/**
 * Specifies all constant values in Tez
 */
@Private
public class TezConstants {


  public static final String TEZ_APPLICATION_MASTER_CLASS =
      "org.apache.tez.dag.app.DAGAppMaster";
  
  /**
   * Command-line argument to be set when running the Tez AM in session mode.
   */
  public static final String TEZ_SESSION_MODE_CLI_OPTION = "session";

  public static final String TEZ_TAR_LR_NAME = "tezlib";
  
  /*
   * Tez AM Service Authorization
   * These are the same as MR which allows Tez to run in secure
   * mode without configuring service ACLs
   */
  public static final String   
  TEZ_AM_SECURITY_SERVICE_AUTHORIZATION_TASK_UMBILICAL =
      "security.job.task.protocol.acl";
  public static final String   
  TEZ_AM_SECURITY_SERVICE_AUTHORIZATION_CLIENT =
      "security.job.client.protocol.acl";

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

  public static final String TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME =
    TezConfiguration.TEZ_SESSION_PREFIX + "local-resources.pb";
  
  public static final String TEZ_APPLICATION_TYPE = "TEZ";

  /**
   * The service id for the NodeManager plugin used to share intermediate data
   * between vertices.
   */
  public static final String TEZ_SHUFFLE_HANDLER_SERVICE_ID = "mapreduce_shuffle";
  
  public static final String TEZ_PREWARM_DAG_NAME_PREFIX = "TezPreWarmDAG";

  public static final String DAG_RECOVERY_DATA_DIR_NAME = "recovery";
  public static final String DAG_RECOVERY_SUMMARY_FILE_SUFFIX = "summary";
  public static final String DAG_RECOVERY_RECOVER_FILE_SUFFIX = ".recovery";


  // Configuration keys used internally and not set by the users
  
  // These are session specific DAG ACL's. Currently here because these can only be specified
  // via code in the API.
  /**
   * DAG view ACLs. This allows the specified users/groups to view the status of the given DAG.
   */
  public static final String TEZ_DAG_VIEW_ACLS = TezConfiguration.TEZ_AM_PREFIX + "dag.view-acls";
  /**
   * DAG modify ACLs. This allows the specified users/groups to run modify operations on the DAG
   * such as killing the DAG.
   */
  public static final String TEZ_DAG_MODIFY_ACLS = TezConfiguration.TEZ_AM_PREFIX + "dag.modify-acls";

  public static final long TEZ_DAG_SLEEP_TIME_BEFORE_EXIT = 5000;
}
