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
package org.apache.tez.mapreduce.processor.reduce;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;
import org.apache.tez.common.MRFrameworkConfigs;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.TestUmbilical;
import org.apache.tez.mapreduce.TezTestUtils;
import org.apache.tez.mapreduce.hadoop.IDConverter;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutputLegacy;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.mapreduce.processor.MapUtils;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.api.impl.EventType;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.apache.tez.runtime.library.input.OrderedGroupedInputLegacy;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;


@SuppressWarnings("deprecation")
public class TestReduceProcessor {
  
  private static final Logger LOG = LoggerFactory.getLogger(TestReduceProcessor.class);

  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null;
  private static Path workDir = null;
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
      workDir =
          new Path(new Path(System.getProperty("test.build.data", "/tmp")),
                   "TestReduceProcessor").makeQualified(localFs);
      LOG.info("Using workDir: " + workDir);
      MapUtils.configureLocalDirs(defaultConf, workDir.toString());
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  public void setUpJobConf(JobConf job) {
    job.set(TezRuntimeFrameworkConfigs.LOCAL_DIRS, workDir.toString());
    job.set(MRConfig.LOCAL_DIR, workDir.toString());
    job.setClass(
        Constants.TEZ_RUNTIME_TASK_OUTPUT_MANAGER,
        TezTaskOutputFiles.class,
        TezTaskOutput.class);
    job.set(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS, MRPartitioner.class.getName());
    job.setNumReduceTasks(1);
  }

  @Before
  @After
  public void cleanup() throws Exception {
    localFs.delete(workDir, true);
  }

  @Test(timeout = 5000)
  public void testReduceProcessor() throws Exception {
    final String dagName = "mrdag0";
    String mapVertexName = MultiStageMRConfigUtil.getInitialMapVertexName();
    String reduceVertexName = MultiStageMRConfigUtil.getFinalReduceVertexName();
    JobConf jobConf = new JobConf(defaultConf);
    setUpJobConf(jobConf);
    
    MRHelpers.translateMRConfToTez(jobConf);
    jobConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);

    jobConf.set(MRFrameworkConfigs.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());
    jobConf.setBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS, false);
    
    Path mapInput = new Path(workDir, "map0");
    MapUtils.generateInputSplit(localFs, workDir, jobConf, mapInput);

    InputSpec mapInputSpec = new InputSpec("NullSrcVertex",
        InputDescriptor.create(MRInputLegacy.class.getName())
            .setUserPayload(UserPayload.create(ByteBuffer.wrap(
                MRRuntimeProtos.MRInputUserPayloadProto.newBuilder()
                    .setConfigurationBytes(TezUtils.createByteStringFromConf(jobConf)).build()
                    .toByteArray()))),
        1);
    OutputSpec mapOutputSpec = new OutputSpec("NullDestVertex", 
        OutputDescriptor.create(OrderedPartitionedKVOutput.class.getName()).
          setUserPayload(TezUtils.createUserPayloadFromConf(jobConf)), 1);
    // Run a map

    TestUmbilical testUmbilical = new TestUmbilical();

    LogicalIOProcessorRuntimeTask mapTask = MapUtils.createLogicalTask(localFs, workDir, jobConf, 0,
        mapInput, testUmbilical, dagName, mapVertexName,
        Collections.singletonList(mapInputSpec),
        Collections.singletonList(mapOutputSpec));

    mapTask.initialize();
    mapTask.run();
    mapTask.close();

    // One VME, One DME
    Assert.assertEquals(2, testUmbilical.getEvents().size());
    Assert.assertEquals(EventType.VERTEX_MANAGER_EVENT, testUmbilical.getEvents().get(0).getEventType());
    Assert.assertEquals(EventType.COMPOSITE_DATA_MOVEMENT_EVENT, testUmbilical.getEvents().get(1).getEventType());

    CompositeDataMovementEvent cdmEvent = (CompositeDataMovementEvent) testUmbilical.getEvents().get(1).getEvent();
    Assert.assertEquals(1, cdmEvent.getCount());
    DataMovementEvent dme = cdmEvent.getEvents().iterator().next();
    dme.setTargetIndex(0);
    
    LOG.info("Starting reduce...");
    
    JobTokenIdentifier identifier = new JobTokenIdentifier(new Text(dagName));
    JobTokenSecretManager jobTokenSecretManager = new JobTokenSecretManager();
    Token<JobTokenIdentifier> shuffleToken = new Token<JobTokenIdentifier>(identifier,
        jobTokenSecretManager);
    shuffleToken.setService(identifier.getJobId());

    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    jobConf.set(MRFrameworkConfigs.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());
    jobConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    FileOutputFormat.setOutputPath(jobConf, new Path(workDir, "output"));
    ProcessorDescriptor reduceProcessorDesc = ProcessorDescriptor.create(
        ReduceProcessor.class.getName()).setUserPayload(
        TezUtils.createUserPayloadFromConf(jobConf));
    
    InputSpec reduceInputSpec = new InputSpec(mapVertexName,
        InputDescriptor.create(OrderedGroupedInputLegacy.class.getName())
            .setUserPayload(TezUtils.createUserPayloadFromConf(jobConf)), 1);
    OutputSpec reduceOutputSpec = new OutputSpec("NullDestinationVertex",
        OutputDescriptor.create(MROutputLegacy.class.getName())
            .setUserPayload(TezUtils.createUserPayloadFromConf(jobConf)), 1);

    // Now run a reduce
    TaskSpec taskSpec = new TaskSpec(
        TezTestUtils.getMockTaskAttemptId(0, 1, 0, 0),
        dagName,
        reduceVertexName, -1,
        reduceProcessorDesc,
        Collections.singletonList(reduceInputSpec),
        Collections.singletonList(reduceOutputSpec), null);

    Map<String, ByteBuffer> serviceConsumerMetadata = new HashMap<String, ByteBuffer>();
    serviceConsumerMetadata.put(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID,
        ShuffleUtils.convertJobTokenToBytes(shuffleToken));
    Map<String, String> serviceProviderEnvMap = new HashMap<String, String>();
    ByteBuffer shufflePortBb = ByteBuffer.allocate(4).putInt(0, 8000);
    AuxiliaryServiceHelper
        .setServiceDataIntoEnv(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID, shufflePortBb,
            serviceProviderEnvMap);

    LogicalIOProcessorRuntimeTask task = new LogicalIOProcessorRuntimeTask(
        taskSpec,
        0,
        jobConf,
        new String[] {workDir.toString()},
        new TestUmbilical(),
        serviceConsumerMetadata,
        serviceProviderEnvMap,
        HashMultimap.<String, String>create(), null, "", new ExecutionContextImpl("localhost"),
        Runtime.getRuntime().maxMemory());

    List<Event> destEvents = new LinkedList<Event>();
    destEvents.add(dme);
    task.initialize();
    OrderedGroupedInputLegacy sortedOut =
        (OrderedGroupedInputLegacy) task.getInputs().values().iterator().next();
    sortedOut.handleEvents(destEvents);
    task.run();
    task.close();

    // MRTask mrTask = (MRTask)t.getProcessor();
    // TODO NEWTEZ Verify the partitioner has not been created
    // Likely not applicable anymore.
    // Assert.assertNull(mrTask.getPartitioner());



    // Only a task commit happens, hence the data is still in the temporary directory.
    Path reduceOutputDir = new Path(new Path(workDir, "output"),
        "_temporary/0/" + IDConverter
            .toMRTaskIdForOutput(TezTestUtils.getMockTaskId(0, 1, 0)));

    Path reduceOutputFile = new Path(reduceOutputDir, "part-v001-o000-00000");

    SequenceFile.Reader reader = new SequenceFile.Reader(localFs,
        reduceOutputFile, jobConf);

    LongWritable key = new LongWritable();
    Text value = new Text();
    long prev = Long.MIN_VALUE;
    while (reader.next(key, value)) {
      if (prev != Long.MIN_VALUE) {
        Assert.assertTrue(prev < key.get());
        prev = key.get();
      }
    }

    reader.close();
  }

}
