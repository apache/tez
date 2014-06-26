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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.RootInputSpecUpdate;
import org.apache.tez.runtime.api.events.RootInputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.api.events.RootInputUpdatePayloadEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class RootInputVertexManager extends VertexManagerPlugin {

  VertexManagerPluginContext context;
  private String configuredInputName;

  @Override
  public void initialize(VertexManagerPluginContext context) {
    this.context = context;
  }

  @Override
  public void onVertexStarted(Map<String, List<Integer>> completions) {
    int numTasks = context.getVertexNumTasks(context.getVertexName());
    List<TaskWithLocationHint> scheduledTasks = Lists.newArrayListWithCapacity(numTasks);
    for (int i=0; i<numTasks; ++i) {
      scheduledTasks.add(new TaskWithLocationHint(new Integer(i), null));
    }
    context.scheduleVertexTasks(scheduledTasks);
  }

  @Override
  public void onSourceTaskCompleted(String srcVertexName, Integer attemptId) {
  }

  @Override
  public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
  }

  @Override
  public void onRootVertexInitialized(String inputName, InputDescriptor inputDescriptor,
      List<Event> events) {
    List<RootInputDataInformationEvent> riEvents = Lists.newLinkedList();
    boolean dataInformationEventSeen = false;
    for (Event event : events) {
      if (event instanceof RootInputConfigureVertexTasksEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        Preconditions.checkState(context.getVertexNumTasks(context.getVertexName()) == -1,
            "Parallelism for the vertex should be set to -1 if the InputInitializer is setting parallelism"
                + ", VertexName: " + context.getVertexName());
        Preconditions.checkState(configuredInputName == null,
            "RootInputVertexManager cannot configure multiple inputs. Use a custom VertexManager"
                + ", VertexName: " + context.getVertexName() + ", ConfiguredInput: "
                + configuredInputName + ", CurrentInput: " + inputName);
        configuredInputName = inputName;
        RootInputConfigureVertexTasksEvent cEvent = (RootInputConfigureVertexTasksEvent) event;
        Map<String, RootInputSpecUpdate> rootInputSpecUpdate = new HashMap<String, RootInputSpecUpdate>();
        rootInputSpecUpdate.put(
            inputName,
            cEvent.getRootInputSpecUpdate() == null ? RootInputSpecUpdate
                .getDefaultSinglePhysicalInputSpecUpdate() : cEvent.getRootInputSpecUpdate());
        context.setVertexParallelism(cEvent.getNumTasks(),
            new VertexLocationHint(cEvent.getTaskLocationHints()), null, rootInputSpecUpdate);
      }
      if (event instanceof RootInputUpdatePayloadEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        inputDescriptor.setUserPayload(((RootInputUpdatePayloadEvent) event)
            .getUserPayload());
      } else if (event instanceof RootInputDataInformationEvent) {
        dataInformationEventSeen = true;
        // # Tasks should have been set by this point.
        Preconditions.checkState(context.getVertexNumTasks(context.getVertexName()) != 0);
        Preconditions.checkState(
            configuredInputName == null || configuredInputName.equals(inputName),
            "RootInputVertexManager cannot configure multiple inputs. Use a custom VertexManager"
                + ", VertexName:" + context.getVertexName() + ", ConfiguredInput: "
                + configuredInputName + ", CurrentInput: " + inputName);
        configuredInputName = inputName;
        
        RootInputDataInformationEvent rEvent = (RootInputDataInformationEvent)event;
        rEvent.setTargetIndex(rEvent.getSourceIndex()); // 1:1 routing
        riEvents.add(rEvent);
      }
    }
    context.addRootInputEvents(inputName, riEvents);
  }
}
