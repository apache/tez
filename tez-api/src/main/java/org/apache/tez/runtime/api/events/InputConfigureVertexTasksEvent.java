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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputSpecUpdate;

/**
 * An event typically sent by the {@link InputInitializer} of a vertex 
 * to configure the tasks of the vertex. It could change the task 
 * placement hints or input specification for the inputs of the tasks
 */
@Unstable
@Public
public class InputConfigureVertexTasksEvent extends Event {

  private final int numTasks;
  private final VertexLocationHint locationHint;
  private final InputSpecUpdate inputSpecUpdate;

  private InputConfigureVertexTasksEvent(int numTasks, VertexLocationHint locationHint,
                                         InputSpecUpdate inputSpecUpdate) {
    this.numTasks = numTasks;
    this.locationHint = locationHint;
    this.inputSpecUpdate = inputSpecUpdate;
  }

  public static InputConfigureVertexTasksEvent create(int numTasks, VertexLocationHint locationHint,
                                                      InputSpecUpdate inputSpecUpdate) {
    return new InputConfigureVertexTasksEvent(numTasks, locationHint, inputSpecUpdate);
  }

  public int getNumTasks() {
    return numTasks;
  }

  public VertexLocationHint getLocationHint() {
    return locationHint;
  }

  public InputSpecUpdate getInputSpecUpdate() {
    return this.inputSpecUpdate;
  }

}
