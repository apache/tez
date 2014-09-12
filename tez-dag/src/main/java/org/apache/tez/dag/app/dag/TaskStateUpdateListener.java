/*
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

package org.apache.tez.dag.app.dag;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.records.TezTaskID;

@InterfaceAudience.Private
/**
 * This class should not be implemented by user facing APIs such as InputInitializer
 */
public interface TaskStateUpdateListener {

  // Internal usage only. Currently only supporting onSuccess notifications for tasks.
  // Exposing the taskID is ok, since this isn't public
  public void onTaskSucceeded(String vertexName, TezTaskID taskId, int attemptId);
}