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
package org.apache.tez.mapreduce.output;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.runtime.api.OutputContext;
import org.junit.Test;


public class TestMROutputLegacy {

  // simulate the behavior of translating MR to DAG using MR old API
  @Test (timeout = 5000)
  public void testOldAPI_MR() throws Exception {
    String outputPath = "/tmp/output";
    JobConf conf = new JobConf();
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class);
    org.apache.hadoop.mapred.SequenceFileOutputFormat.setOutputPath(conf, new Path(outputPath));
    // the output is attached to reducer
    conf.setBoolean(MRConfig.IS_MAP_PROCESSOR, false);
    UserPayload vertexPayload = TezUtils.createUserPayloadFromConf(conf);
    OutputDescriptor od = OutputDescriptor.create(MROutputLegacy.class.getName())
        .setUserPayload(vertexPayload);
    DataSinkDescriptor sink = DataSinkDescriptor.create(od,
        OutputCommitterDescriptor.create(MROutputCommitter.class.getName()), null);

    OutputContext outputContext = createMockOutputContext(sink.getOutputDescriptor().getUserPayload());
    MROutputLegacy output = new MROutputLegacy(outputContext, 2);
    output.initialize();
    assertEquals(false, output.useNewApi);
    assertEquals(org.apache.hadoop.mapred.SequenceFileOutputFormat.class, output.oldOutputFormat.getClass());
    assertNull(output.newOutputFormat);
    assertEquals(NullWritable.class, output.oldApiTaskAttemptContext.getOutputKeyClass());
    assertEquals(Text.class, output.oldApiTaskAttemptContext.getOutputValueClass());
    assertNull(output.newApiTaskAttemptContext);
    assertNotNull(output.oldRecordWriter);
    assertNull(output.newRecordWriter);
    assertEquals(org.apache.hadoop.mapred.FileOutputCommitter.class, output.committer.getClass());
  }

  // simulate the behavior of translating MR to DAG using MR new API
  @Test (timeout = 5000)
  public void testNewAPI_MR() throws Exception {
    String outputPath = "/tmp/output";
    Job job = Job.getInstance();
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
    job.getConfiguration().setBoolean("mapred.reducer.new-api", true);
    // the output is attached to reducer
    job.getConfiguration().setBoolean(MRConfig.IS_MAP_PROCESSOR, false);
    UserPayload vertexPayload = TezUtils.createUserPayloadFromConf(job.getConfiguration());
    OutputDescriptor od = OutputDescriptor.create(MROutputLegacy.class.getName())
        .setUserPayload(vertexPayload);
    DataSinkDescriptor sink = DataSinkDescriptor.create(od,
        OutputCommitterDescriptor.create(MROutputCommitter.class.getName()), null);

    OutputContext outputContext = createMockOutputContext(sink.getOutputDescriptor().getUserPayload());
    MROutputLegacy output = new MROutputLegacy(outputContext, 2);
    output.initialize();
    assertEquals(true, output.useNewApi);
    assertEquals(SequenceFileOutputFormat.class, output.newOutputFormat.getClass());
    assertNull(output.oldOutputFormat);
    assertEquals(NullWritable.class, output.newApiTaskAttemptContext.getOutputKeyClass());
    assertEquals(Text.class, output.newApiTaskAttemptContext.getOutputValueClass());
    assertNull(output.oldApiTaskAttemptContext);
    assertNotNull(output.newRecordWriter);
    assertNull(output.oldRecordWriter);
    assertEquals(FileOutputCommitter.class, output.committer.getClass());
  }

  // simulate the behavior of translating Mapper-only job to DAG using MR old API
  @Test (timeout = 5000)
  public void testOldAPI_MapperOnly() throws Exception {
    String outputPath = "/tmp/output";
    JobConf conf = new JobConf();
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class);
    org.apache.hadoop.mapred.SequenceFileOutputFormat.setOutputPath(conf, new Path(outputPath));
    // the output is attached to mapper
    conf.setBoolean(MRConfig.IS_MAP_PROCESSOR, true);
    UserPayload vertexPayload = TezUtils.createUserPayloadFromConf(conf);
    OutputDescriptor od = OutputDescriptor.create(MROutputLegacy.class.getName())
        .setUserPayload(vertexPayload);
    DataSinkDescriptor sink = DataSinkDescriptor.create(od,
        OutputCommitterDescriptor.create(MROutputCommitter.class.getName()), null);

    OutputContext outputContext = createMockOutputContext(sink.getOutputDescriptor().getUserPayload());
    MROutputLegacy output = new MROutputLegacy(outputContext, 2);
    output.initialize();
    assertEquals(false, output.useNewApi);
    assertEquals(org.apache.hadoop.mapred.SequenceFileOutputFormat.class, output.oldOutputFormat.getClass());
    assertNull(output.newOutputFormat);
    assertEquals(NullWritable.class, output.oldApiTaskAttemptContext.getOutputKeyClass());
    assertEquals(Text.class, output.oldApiTaskAttemptContext.getOutputValueClass());
    assertNull(output.newApiTaskAttemptContext);
    assertNotNull(output.oldRecordWriter);
    assertNull(output.newRecordWriter);
    assertEquals(org.apache.hadoop.mapred.FileOutputCommitter.class, output.committer.getClass());
  }

  //simulate the behavior of translating mapper-only job to DAG using MR new API
  @Test (timeout = 5000)
  public void testNewAPI_MapperOnly() throws Exception {
    String outputPath = "/tmp/output";
    Job job = Job.getInstance();
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
    job.getConfiguration().setBoolean("mapred.mapper.new-api", true);
    // the output is attached to mapper
    job.getConfiguration().setBoolean(MRConfig.IS_MAP_PROCESSOR, true);
    UserPayload vertexPayload = TezUtils.createUserPayloadFromConf(job.getConfiguration());
    OutputDescriptor od = OutputDescriptor.create(MROutputLegacy.class.getName())
        .setUserPayload(vertexPayload);
    DataSinkDescriptor sink = DataSinkDescriptor.create(od,
        OutputCommitterDescriptor.create(MROutputCommitter.class.getName()), null);

    OutputContext outputContext = createMockOutputContext(sink.getOutputDescriptor().getUserPayload());
    MROutputLegacy output = new MROutputLegacy(outputContext, 2);
    output.initialize();
    assertEquals(true, output.useNewApi);
    assertEquals(SequenceFileOutputFormat.class, output.newOutputFormat.getClass());
    assertNull(output.oldOutputFormat);
    assertEquals(NullWritable.class, output.newApiTaskAttemptContext.getOutputKeyClass());
    assertEquals(Text.class, output.newApiTaskAttemptContext.getOutputValueClass());
    assertNull(output.oldApiTaskAttemptContext);
    assertNotNull(output.newRecordWriter);
    assertNull(output.oldRecordWriter);
    assertEquals(FileOutputCommitter.class, output.committer.getClass());
  }

  private OutputContext createMockOutputContext(UserPayload payload) {
    OutputContext outputContext = mock(OutputContext.class);
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    when(outputContext.getUserPayload()).thenReturn(payload);
    when(outputContext.getApplicationId()).thenReturn(appId);
    when(outputContext.getTaskVertexIndex()).thenReturn(1);
    when(outputContext.getTaskAttemptNumber()).thenReturn(1);
    when(outputContext.getCounters()).thenReturn(new TezCounters());
    return outputContext;
  }
}
