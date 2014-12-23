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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.LinkedList;
import java.util.List;

import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.junit.Test;

public class TestRootInputVertexManager {

  @Test(timeout = 5000)
  public void testEventsFromMultipleInputs() {

    VertexManagerPluginContext context = mock(VertexManagerPluginContext.class);
    doReturn("vertex1").when(context).getVertexName();
    doReturn(1).when(context).getVertexNumTasks(eq("vertex1"));

    RootInputVertexManager rootInputVertexManager = new RootInputVertexManager(context);
    rootInputVertexManager.initialize();

    InputDescriptor id1 = mock(InputDescriptor.class);
    List<Event> events1 = new LinkedList<Event>();
    InputDataInformationEvent diEvent11 = InputDataInformationEvent.createWithSerializedPayload(0,
        null);
    events1.add(diEvent11);
    rootInputVertexManager.onRootVertexInitialized("input1", id1, events1);
    // All good so far, single input only.

    InputDescriptor id2 = mock(InputDescriptor.class);
    List<Event> events2 = new LinkedList<Event>();
    InputDataInformationEvent diEvent21 = InputDataInformationEvent.createWithSerializedPayload(0,
        null);
    events2.add(diEvent21);
    try {
      // Should fail due to second input
      rootInputVertexManager.onRootVertexInitialized("input2", id2, events2);
      fail("Expecting failure in case of multiple inputs attempting to send events");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().startsWith(
          "RootInputVertexManager cannot configure multiple inputs. Use a custom VertexManager"));
    }
  }

  @Test(timeout = 5000)
  public void testConfigureFromMultipleInputs() {

    VertexManagerPluginContext context = mock(VertexManagerPluginContext.class);
    doReturn("vertex1").when(context).getVertexName();
    doReturn(-1).when(context).getVertexNumTasks(eq("vertex1"));

    RootInputVertexManager rootInputVertexManager = new RootInputVertexManager(context);
    rootInputVertexManager.initialize();

    InputDescriptor id1 = mock(InputDescriptor.class);
    List<Event> events1 = new LinkedList<Event>();
    InputConfigureVertexTasksEvent diEvent11 = InputConfigureVertexTasksEvent.create(1, null,
        null);
    events1.add(diEvent11);
    rootInputVertexManager.onRootVertexInitialized("input1", id1, events1);
    // All good so far, single input only.

    InputDescriptor id2 = mock(InputDescriptor.class);
    List<Event> events2 = new LinkedList<Event>();
    InputConfigureVertexTasksEvent diEvent21 = InputConfigureVertexTasksEvent.create(1, null,
        null);
    events2.add(diEvent21);
    try {
      // Should fail due to second input
      rootInputVertexManager.onRootVertexInitialized("input2", id2, events2);
      fail("Expecting failure in case of multiple inputs attempting to send events");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().startsWith(
          "RootInputVertexManager cannot configure multiple inputs. Use a custom VertexManager"));
    }
  }

}
