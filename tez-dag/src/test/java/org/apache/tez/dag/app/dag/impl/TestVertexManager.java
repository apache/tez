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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedList;
import java.util.List;

import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.Test;

public class TestVertexManager {

  @Test
  public void testOnRootVertexInitialized() {
    Vertex mockVertex = mock(Vertex.class, RETURNS_DEEP_STUBS);
    AppContext mockAppContext = mock(AppContext.class, RETURNS_DEEP_STUBS);
    doReturn("vertex1").when(mockVertex).getName();
    when(
        mockAppContext.getCurrentDAG().getVertex(any(String.class))
            .getTotalTasks()).thenReturn(1);

    VertexManager vm =
        new VertexManager(
            VertexManagerPluginDescriptor.create(RootInputVertexManager.class
                .getName()), mockVertex, mockAppContext);
    vm.initialize();
    InputDescriptor id1 = mock(InputDescriptor.class);
    List<Event> events1 = new LinkedList<Event>();
    InputDataInformationEvent diEvent1 =
        InputDataInformationEvent.createWithSerializedPayload(0, null);
    events1.add(diEvent1);
    List<TezEvent> tezEvents1 =
        vm.onRootVertexInitialized("input1", id1, events1);
    assertEquals(1, tezEvents1.size());
    assertEquals(diEvent1, tezEvents1.get(0).getEvent());

    InputDescriptor id2 = mock(InputDescriptor.class);
    List<Event> events2 = new LinkedList<Event>();
    InputDataInformationEvent diEvent2 =
        InputDataInformationEvent.createWithSerializedPayload(0, null);
    events2.add(diEvent2);
    List<TezEvent> tezEvents2 =
        vm.onRootVertexInitialized("input1", id2, events2);
    assertEquals(tezEvents2.size(), 1);
    assertEquals(diEvent2, tezEvents2.get(0).getEvent());
  }
}
