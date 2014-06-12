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
import org.apache.tez.runtime.api.events.RootInputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.junit.Test;

public class TestRootInputVertexManager {

  @Test
  public void testEventsFromMultipleInputs() {

    VertexManagerPluginContext context = mock(VertexManagerPluginContext.class);
    doReturn("vertex1").when(context).getVertexName();
    doReturn(1).when(context).getVertexNumTasks(eq("vertex1"));

    RootInputVertexManager rootInputVertexManager = new RootInputVertexManager();
    rootInputVertexManager.initialize(context);

    InputDescriptor id1 = mock(InputDescriptor.class);
    List<Event> events1 = new LinkedList<Event>();
    RootInputDataInformationEvent diEvent11 = new RootInputDataInformationEvent(0, null);
    events1.add(diEvent11);
    rootInputVertexManager.onRootVertexInitialized("input1", id1, events1);
    // All good so far, single input only.

    InputDescriptor id2 = mock(InputDescriptor.class);
    List<Event> events2 = new LinkedList<Event>();
    RootInputDataInformationEvent diEvent21 = new RootInputDataInformationEvent(0, null);
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

  @Test
  public void testConfigureFromMultipleInputs() {

    VertexManagerPluginContext context = mock(VertexManagerPluginContext.class);
    doReturn("vertex1").when(context).getVertexName();
    doReturn(-1).when(context).getVertexNumTasks(eq("vertex1"));

    RootInputVertexManager rootInputVertexManager = new RootInputVertexManager();
    rootInputVertexManager.initialize(context);

    InputDescriptor id1 = mock(InputDescriptor.class);
    List<Event> events1 = new LinkedList<Event>();
    RootInputConfigureVertexTasksEvent diEvent11 = new RootInputConfigureVertexTasksEvent(1, null,
        null);
    events1.add(diEvent11);
    rootInputVertexManager.onRootVertexInitialized("input1", id1, events1);
    // All good so far, single input only.

    InputDescriptor id2 = mock(InputDescriptor.class);
    List<Event> events2 = new LinkedList<Event>();
    RootInputConfigureVertexTasksEvent diEvent21 = new RootInputConfigureVertexTasksEvent(1, null,
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
