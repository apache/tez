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

package org.apache.tez.runtime.library.output;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.tez.common.TezSharedExecutor;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezOutputContextImpl;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.common.resources.MemoryDistributor;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.apache.tez.runtime.library.testutils.KVDataGen;
import org.apache.tez.runtime.library.testutils.KVDataGen.KVPair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@SuppressWarnings("rawtypes")
public class TestOnFileUnorderedKVOutput {

  private static final Logger LOG = LoggerFactory.getLogger(TestOnFileUnorderedKVOutput.class);

  private static Configuration defaultConf = new Configuration();
  private static FileSystem localFs = null;
  private static Path workDir = null;
  private static final int shufflePort = 2112;

  LogicalIOProcessorRuntimeTask task;

  static {
    defaultConf.set("fs.defaultFS", "file:///");
    try {
      localFs = FileSystem.getLocal(defaultConf);
      workDir = new Path(
          new Path(System.getProperty("test.build.data", "/tmp")), TestOnFileUnorderedKVOutput.class.getName())
          .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
      LOG.info("Using workDir: " + workDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setup() throws Exception {
    localFs.mkdirs(workDir);
  }

  @After
  public void cleanup() throws Exception {
    localFs.delete(workDir, true);
  }

  @Test(timeout = 5000)
  public void testGeneratedDataMovementEvent() throws Exception {
    Configuration conf = new Configuration();
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, IntWritable.class.getName());

    TezSharedExecutor sharedExecutor = new TezSharedExecutor(conf);
    OutputContext outputContext = createOutputContext(conf, new Configuration(false), sharedExecutor);

    UnorderedKVOutput kvOutput = new UnorderedKVOutput(outputContext, 1);

    List<Event> events = null;
    events = kvOutput.initialize();
    kvOutput.start();
    assertTrue(events != null && events.size() == 0);

    KeyValueWriter kvWriter = kvOutput.getWriter();
    List<KVPair> data = KVDataGen.generateTestData(true, 0);
    for (KVPair kvp : data) {
      kvWriter.write(kvp.getKey(), kvp.getvalue());
    }

    events = kvOutput.close();
    assertEquals(45, task.getTaskStatistics().getIOStatistics().values().iterator().next().getDataSize());
    assertEquals(5, task.getTaskStatistics().getIOStatistics().values().iterator().next().getItemsProcessed());
    assertTrue(events != null && events.size() == 2);
    CompositeDataMovementEvent dmEvent = (CompositeDataMovementEvent)events.get(1);

    assertEquals("Invalid source index", 0, dmEvent.getSourceIndexStart());

    DataMovementEventPayloadProto shufflePayload = DataMovementEventPayloadProto
        .parseFrom(ByteString.copyFrom(dmEvent.getUserPayload()));

    assertFalse(shufflePayload.hasEmptyPartitions());
    assertEquals(outputContext.getUniqueIdentifier(), shufflePayload.getPathComponent());
    assertEquals(shufflePort, shufflePayload.getPort());
    assertEquals("localhost", shufflePayload.getHost());
    sharedExecutor.shutdownNow();
  }

  @Test
  public void testMergeConfig() throws Exception {
    Configuration baseConf = new Configuration(false);
    baseConf.set("local-key", "local-value");

    Configuration payloadConf = new Configuration(false);
    payloadConf.set("base-key", "base-value");

    TezSharedExecutor sharedExecutor = new TezSharedExecutor(baseConf);
    OutputContext outputContext = createOutputContext(payloadConf, baseConf, sharedExecutor);

    UnorderedKVOutput kvOutput = new UnorderedKVOutput(outputContext, 1);

    kvOutput.initialize();

    Configuration mergedConf = kvOutput.conf;
    assertEquals("local-value", mergedConf.get("local-key"));
    assertEquals("base-value", mergedConf.get("base-key"));
  }

  @Test(timeout = 30000)
  @SuppressWarnings("unchecked")
  public void testWithPipelinedShuffle() throws Exception {
    Configuration conf = new Configuration();
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, Text.class.getName());
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, IntWritable.class.getName());
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED, true);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT, false);

    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB, 1);

    TezSharedExecutor sharedExecutor = new TezSharedExecutor(conf);
    OutputContext outputContext = createOutputContext(conf, new Configuration(false), sharedExecutor);

    UnorderedKVOutput kvOutput = new UnorderedKVOutput(outputContext, 1);

    List<Event> events = null;
    events = kvOutput.initialize();
    kvOutput.start();
    assertTrue(events != null && events.size() == 0);

    KeyValueWriter kvWriter = kvOutput.getWriter();
    for (int i = 0; i < 500; i++) {
      kvWriter.write(new Text(RandomStringUtils.randomAscii(10000)), new IntWritable(i));
    }

    events = kvOutput.close();
    //When pipelining is on, all events are sent out within writer itself.
    assertTrue(events != null && events.size() == 0);

    //ensure that data is sent via outputContext.
    ArgumentCaptor<List> eventsCaptor = ArgumentCaptor.forClass(List.class);
    verify(outputContext, atLeast(1)).sendEvents(eventsCaptor.capture());
    events = eventsCaptor.getValue();


    CompositeDataMovementEvent dmEvent = (CompositeDataMovementEvent)events.get(1);
    assertEquals("Invalid source index", 0, dmEvent.getSourceIndexStart());

    DataMovementEventPayloadProto shufflePayload = DataMovementEventPayloadProto
        .parseFrom(ByteString.copyFrom(dmEvent.getUserPayload()));

    assertTrue(shufflePayload.hasLastEvent());

    assertFalse(shufflePayload.hasEmptyPartitions());
    assertEquals(shufflePort, shufflePayload.getPort());
    assertEquals("localhost", shufflePayload.getHost());
    sharedExecutor.shutdownNow();
  }

  private OutputContext createOutputContext(Configuration payloadConf, Configuration baseConf,
      TezSharedExecutor sharedExecutor) throws IOException {
    int appAttemptNumber = 1;
    TezUmbilical tezUmbilical = mock(TezUmbilical.class);
    String dagName = "currentDAG";
    String taskVertexName = "currentVertex";
    String destinationVertexName = "destinationVertex";
    TezDAGID dagID = TezDAGID.getInstance("2000", 1, 1);
    TezVertexID vertexID = TezVertexID.getInstance(dagID, 1);
    TezTaskID taskID = TezTaskID.getInstance(vertexID, 1);
    TezTaskAttemptID taskAttemptID = TezTaskAttemptID.getInstance(taskID, 1);
    UserPayload userPayload = TezUtils.createUserPayloadFromConf(payloadConf);
    
    TaskSpec mockSpec = mock(TaskSpec.class);
    when(mockSpec.getInputs()).thenReturn(Collections.singletonList(mock(InputSpec.class)));
    when(mockSpec.getOutputs()).thenReturn(Collections.singletonList(mock(OutputSpec.class)));
    task = new LogicalIOProcessorRuntimeTask(mockSpec, appAttemptNumber, new Configuration(),
        new String[]{"/"}, tezUmbilical, null, null, null, null, "", null, 1024, false,
        new DefaultHadoopShim(), sharedExecutor);

    LogicalIOProcessorRuntimeTask runtimeTask = spy(task);
    
    Map<String, String> auxEnv = new HashMap<String, String>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(shufflePort);
    bb.position(0);
    AuxiliaryServiceHelper.setServiceDataIntoEnv(payloadConf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
        TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT), bb, auxEnv);


    OutputDescriptor outputDescriptor = mock(OutputDescriptor.class);
    when(outputDescriptor.getClassName()).thenReturn("OutputDescriptor");

    OutputContext realOutputContext = new TezOutputContextImpl(baseConf, new String[] {workDir.toString()},
        appAttemptNumber, tezUmbilical, dagName, taskVertexName, destinationVertexName,
        -1, taskAttemptID, 0, userPayload, runtimeTask,
        null, auxEnv, new MemoryDistributor(1, 1, payloadConf), outputDescriptor, null,
        new ExecutionContextImpl("localhost"), 2048, new TezSharedExecutor(defaultConf));
    verify(runtimeTask, times(1)).addAndGetTezCounter(destinationVertexName);
    verify(runtimeTask, times(1)).getTaskStatistics();
    // verify output stats object got created
    Assert.assertTrue(task.getTaskStatistics().getIOStatistics().containsKey(destinationVertexName));
    OutputContext outputContext = spy(realOutputContext);
    doAnswer(new Answer() {
      @Override public Object answer(InvocationOnMock invocation) throws Throwable {
        long requestedSize = (Long) invocation.getArguments()[0];
        MemoryUpdateCallbackHandler callback = (MemoryUpdateCallbackHandler) invocation
            .getArguments()[1];
        callback.memoryAssigned(requestedSize);
        return null;
      }
    }).when(outputContext).requestInitialMemory(anyLong(), any(MemoryUpdateCallback.class));

    return outputContext;
  }
}
