/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.api.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.InputReadyTracker;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.apache.tez.runtime.common.resources.MemoryDistributor;
import org.junit.Test;

public class TestProcessorContext {

  @Test (timeout = 5000)
  public void testDagNumber() {
    String[] localDirs = new String[] {"dummyLocalDir"};
    int appAttemptNumber = 1;
    TezUmbilical tezUmbilical = mock(TezUmbilical.class);
    String dagName = "DAG_NAME";
    String vertexName = "VERTEX_NAME";
    int vertexParallelism = 20;
    int dagNumber = 52;
    ApplicationId appId = ApplicationId.newInstance(10000, 13);
    TezDAGID dagId = TezDAGID.getInstance(appId, dagNumber);
    TezVertexID vertexId = TezVertexID.getInstance(dagId, 6);
    TezTaskID taskId = TezTaskID.getInstance(vertexId, 4);
    TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, 2);

    LogicalIOProcessorRuntimeTask runtimeTask = mock(LogicalIOProcessorRuntimeTask.class);
    doReturn(new TezCounters()).when(runtimeTask).addAndGetTezCounter(any(String.class));
    Map<String, ByteBuffer> serviceConsumerMetadata = Maps.newHashMap();
    Map<String, String> auxServiceEnv = Maps.newHashMap();
    MemoryDistributor memDist = mock(MemoryDistributor.class);
    ProcessorDescriptor processorDesc = mock(ProcessorDescriptor.class);
    InputReadyTracker inputReadyTracker = mock(InputReadyTracker.class);
    ObjectRegistry objectRegistry = new ObjectRegistryImpl();
    ExecutionContext execContext = new ExecutionContextImpl("localhost");
    long memAvailable = 10000l;

    TezProcessorContextImpl procContext =
        new TezProcessorContextImpl(
            new Configuration(),
            localDirs,
            appAttemptNumber,
            tezUmbilical,
            dagName,
            vertexName,
            vertexParallelism,
            taskAttemptId,
            null,
            runtimeTask,
            serviceConsumerMetadata,
            auxServiceEnv,
            memDist,
            processorDesc,
            inputReadyTracker,
            objectRegistry,
            execContext,
            memAvailable);

    assertEquals(dagNumber, procContext.getDagIdentifier());
    assertEquals(appAttemptNumber, procContext.getDAGAttemptNumber());
    assertEquals(appId, procContext.getApplicationId());
    assertEquals(dagName, procContext.getDAGName());
    assertEquals(vertexName, procContext.getTaskVertexName());
    assertEquals(vertexId.getId(), procContext.getTaskVertexIndex());
    assertTrue(Arrays.equals(localDirs, procContext.getWorkDirs()));

  }
}
