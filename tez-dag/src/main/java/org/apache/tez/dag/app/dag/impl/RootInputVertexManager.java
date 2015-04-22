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
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputUpdatePayloadEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class RootInputVertexManager extends ImmediateStartVertexManager {

  private static final Logger LOG = 
      LoggerFactory.getLogger(RootInputVertexManager.class);
  
  private String configuredInputName;

  public RootInputVertexManager(VertexManagerPluginContext context) {
    super(context);
  }


  @Override
  public void onRootVertexInitialized(String inputName, InputDescriptor inputDescriptor,
      List<Event> events) {
    List<InputDataInformationEvent> riEvents = Lists.newLinkedList();
    boolean dataInformationEventSeen = false;
    for (Event event : events) {
      if (event instanceof InputConfigureVertexTasksEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        Preconditions.checkState(getContext().getVertexNumTasks(getContext().getVertexName()) == -1,
            "Parallelism for the vertex should be set to -1 if the InputInitializer is setting parallelism"
                + ", VertexName: " + getContext().getVertexName());
        Preconditions.checkState(configuredInputName == null,
            "RootInputVertexManager cannot configure multiple inputs. Use a custom VertexManager"
                + ", VertexName: " + getContext().getVertexName() + ", ConfiguredInput: "
                + configuredInputName + ", CurrentInput: " + inputName);
        configuredInputName = inputName;
        InputConfigureVertexTasksEvent cEvent = (InputConfigureVertexTasksEvent) event;
        Map<String, InputSpecUpdate> rootInputSpecUpdate = new HashMap<String, InputSpecUpdate>();
        rootInputSpecUpdate.put(
            inputName,
            cEvent.getInputSpecUpdate() == null ? InputSpecUpdate
                .getDefaultSinglePhysicalInputSpecUpdate() : cEvent.getInputSpecUpdate());
        getContext().reconfigureVertex(rootInputSpecUpdate, cEvent.getLocationHint(),
              cEvent.getNumTasks());
      }
      if (event instanceof InputUpdatePayloadEvent) {
        // No tasks should have been started yet. Checked by initial state check.
        Preconditions.checkState(dataInformationEventSeen == false);
        inputDescriptor.setUserPayload(UserPayload.create(
            ((InputUpdatePayloadEvent) event).getUserPayload()));
      } else if (event instanceof InputDataInformationEvent) {
        dataInformationEventSeen = true;
        // # Tasks should have been set by this point.
        Preconditions.checkState(getContext().getVertexNumTasks(getContext().getVertexName()) != 0);
        Preconditions.checkState(
            configuredInputName == null || configuredInputName.equals(inputName),
            "RootInputVertexManager cannot configure multiple inputs. Use a custom VertexManager"
                + ", VertexName:" + getContext().getVertexName() + ", ConfiguredInput: "
                + configuredInputName + ", CurrentInput: " + inputName);
        configuredInputName = inputName;
        
        InputDataInformationEvent rEvent = (InputDataInformationEvent)event;
        rEvent.setTargetIndex(rEvent.getSourceIndex()); // 1:1 routing
        riEvents.add(rEvent);
      }
    }
    getContext().addRootInputEvents(inputName, riEvents);
  }
}
