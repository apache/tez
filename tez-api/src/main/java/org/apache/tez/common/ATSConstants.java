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

package org.apache.tez.common;

import org.apache.hadoop.classification.InterfaceAudience.Private;

@Private
public class ATSConstants {

  // TODO remove once YARN exposes proper constants

  /* Top level keys */
  public static final String ENTITIES = "entities";
  public static final String ENTITY = "entity";
  public static final String ENTITY_TYPE = "entitytype";
  public static final String EVENTS = "events";
  public static final String EVENT_TYPE = "eventtype";
  public static final String TIMESTAMP = "ts";
  public static final String EVENT_INFO = "eventinfo";
  public static final String RELATED_ENTITIES = "relatedEntities";
  public static final String PRIMARY_FILTERS = "primaryfilters";
  public static final String SECONDARY_FILTERS = "secondaryfilters";
  public static final String OTHER_INFO = "otherinfo";

  /* Section for related entities */
  public static final String APPLICATION_ID = "applicationId";
  public static final String APPLICATION_ATTEMPT_ID = "applicationAttemptId";
  public static final String CONTAINER_ID = "containerId";
  public static final String NODE_ID = "nodeId";
  public static final String USER = "user";

  /* Keys used in other info */
  public static final String APP_SUBMIT_TIME = "appSubmitTime";

  /* Tez-specific info */
  public static final String DAG_PLAN = "dagPlan";
  public static final String DAG_NAME = "dagName";
  public static final String VERTEX_NAME = "vertexName";
  public static final String SCHEDULED_TIME = "scheduledTime";
  public static final String INIT_REQUESTED_TIME = "initRequestedTime";
  public static final String INIT_TIME = "initTime";
  public static final String START_REQUESTED_TIME = "startRequestedTime";
  public static final String START_TIME = "startTime";
  public static final String FINISH_TIME = "endTime";
  public static final String TIME_TAKEN = "timeTaken";
  public static final String STATUS = "status";
  public static final String DIAGNOSTICS = "diagnostics";
  public static final String COUNTERS = "counters";
  public static final String STATS = "stats";
  public static final String NUM_TASKS = "numTasks";
  public static final String NUM_COMPLETED_TASKS = "numCompletedTasks";
  public static final String NUM_SUCCEEDED_TASKS = "numSucceededTasks";
  public static final String NUM_FAILED_TASKS = "numFailedTasks";
  public static final String NUM_KILLED_TASKS = "numKilledTasks";
  public static final String PROCESSOR_CLASS_NAME = "processorClassName";
  public static final String IN_PROGRESS_LOGS_URL = "inProgressLogsURL";
  public static final String COMPLETED_LOGS_URL = "completedLogsURL";
  public static final String EXIT_STATUS = "exitStatus";

  /* Counters-related keys */
  public static final String COUNTER_GROUPS = "counterGroups";
  public static final String COUNTER_GROUP_NAME = "counterGroupName";
  public static final String COUNTER_GROUP_DISPLAY_NAME = "counterGroupDisplayName";
  public static final String COUNTER_NAME = "counterName";
  public static final String COUNTER_DISPLAY_NAME = "counterDisplayName";
  public static final String COUNTER_VALUE = "counterValue";

  /* Url related */
  public static final String RESOURCE_URI_BASE = "/ws/v1/timeline";
  public static final String TEZ_DAG_ID = "TEZ_DAG_ID";
  public static final String TEZ_VERTEX_ID = "TEZ_VERTEX_ID";

  /* In Yarn but not present in 2.2 */
  public static final String TIMELINE_SERVICE_WEBAPP_HTTP_ADDRESS_CONF_NAME =
      "yarn.timeline-service.webapp.address";
  public static final String TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS_CONF_NAME =
      "yarn.timeline-service.webapp.https.address";
}
