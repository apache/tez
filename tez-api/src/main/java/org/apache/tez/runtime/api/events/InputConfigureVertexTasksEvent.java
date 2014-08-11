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

package org.apache.tez.runtime.api.events;

import java.util.List;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputSpecUpdate;

@Unstable
public class InputConfigureVertexTasksEvent extends Event {

  private final int numTasks;
  private final List<TaskLocationHint> taskLocationHints;
  private final InputSpecUpdate inputSpecUpdate;

  public InputConfigureVertexTasksEvent(int numTasks, List<TaskLocationHint> locationHints,
      InputSpecUpdate inputSpecUpdate) {
    this.numTasks = numTasks;
    this.taskLocationHints = locationHints;
    this.inputSpecUpdate = inputSpecUpdate;
  }

  public int getNumTasks() {
    return numTasks;
  }

  public List<TaskLocationHint> getTaskLocationHints() {
    return taskLocationHints;
  }

  public InputSpecUpdate getInputSpecUpdate() {
    return this.inputSpecUpdate;
  }

}
