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

package org.apache.tez.dag.app.dag.impl;

import java.util.List;
import java.util.Map;

import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.collect.Lists;

/**
 * Starts all tasks immediately on vertex start
 */
public class ImmediateStartVertexManager extends VertexManagerPlugin {

  public ImmediateStartVertexManager(VertexManagerPluginContext context) {
    super(context);
  }

  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    int numTasks = getContext().getVertexNumTasks(getContext().getVertexName());
    List<TaskWithLocationHint> scheduledTasks = Lists.newArrayListWithCapacity(numTasks);
    for (int i=0; i<numTasks; ++i) {
      scheduledTasks.add(new TaskWithLocationHint(new Integer(i), null));
    }
    getContext().scheduleVertexTasks(scheduledTasks);
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer attemptId) {
  }

  @Override
  public void initialize() {
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  @Override
  public void onRootVertexInitialized(String inputName,
      InputDescriptor inputDescriptor, List<Event> events) {
  }

}
