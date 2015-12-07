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

package org.apache.tez.history.parser.datamodel;

import org.apache.tez.common.ATSConstants;

import static org.apache.hadoop.classification.InterfaceAudience.Private;

@Private
public class Constants extends ATSConstants {

  public static final String EVENT_TIME_STAMP = "timestamp";
  public static final String TEZ_APPLICATION = "TEZ_APPLICATION";
  public static final String TEZ_TASK_ID = "TEZ_TASK_ID";
  public static final String TEZ_TASK_ATTEMPT_ID = "TEZ_TASK_ATTEMPT_ID";

  public static final String EDGE_ID = "edgeId";
  public static final String INPUT_VERTEX_NAME = "inputVertexName";
  public static final String OUTPUT_VERTEX_NAME = "outputVertexName";
  public static final String DATA_MOVEMENT_TYPE = "dataMovementType";
  public static final String EDGE_SOURCE_CLASS = "edgeSourceClass";
  public static final String EDGE_DESTINATION_CLASS = "edgeDestinationClass";
  public static final String INPUT_PAYLOAD_TEXT = "inputUserPayloadAsText";
  public static final String OUTPUT_PAYLOAD_TEXT = "outputUserPayloadAsText";

  public static final String EDGES = "edges";
  public static final String OUT_EDGE_IDS = "outEdgeIds";
  public static final String IN_EDGE_IDS = "inEdgeIds";
  public static final String ADDITIONAL_INPUTS = "additionalInputs";
  public static final String ADDITIONAL_OUTPUTS = "additionalOutputs";

  public static final String NAME = "name";
  public static final String CLASS = "class";
  public static final String INITIALIZER = "initializer";
  public static final String USER_PAYLOAD_TEXT = "userPayloadAsText";

  public static final String DAG_CONTEXT = "dagContext";

  //constants for ATS data export
  public static final String DAG = "dag";
  public static final String VERTICES = "vertices";
  public static final String TASKS = "tasks";
  public static final String TASK_ATTEMPTS = "task_attempts";
  public static final String APPLICATION = "application";



}
