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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.runtime.api.OutputContext;
import org.junit.Test;


public class TestMROutput {

  @Test(timeout = 5000)
  public void testNewAPI_TextOutputFormat() throws Exception {
    String outputPath = "/tmp/output";
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.IS_MAP_PROCESSOR, true);
    DataSinkDescriptor dataSink = MROutput
        .createConfigBuilder(conf, TextOutputFormat.class, outputPath)
        .build();

    OutputContext outputContext = createMockOutputContext(dataSink.getOutputDescriptor().getUserPayload());
    MROutput output = new MROutput(outputContext, 2);
    output.initialize();

    assertEquals(true, output.isMapperOutput);
    assertEquals(true, output.useNewApi);
    assertEquals(TextOutputFormat.class, output.newOutputFormat.getClass());
    assertNull(output.oldOutputFormat);
    assertNotNull(output.newApiTaskAttemptContext);
    assertNull(output.oldApiTaskAttemptContext);
    assertNotNull(output.newRecordWriter);
    assertNull(output.oldRecordWriter);
    assertEquals(FileOutputCommitter.class, output.committer.getClass());
  }

  @Test(timeout = 5000)
  public void testOldAPI_TextOutputFormat() throws Exception {
    String outputPath = "/tmp/output";
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.IS_MAP_PROCESSOR, false);
    DataSinkDescriptor dataSink = MROutput
        .createConfigBuilder(conf, org.apache.hadoop.mapred.TextOutputFormat.class, outputPath)
        .build();

    OutputContext outputContext = createMockOutputContext(dataSink.getOutputDescriptor().getUserPayload());
    MROutput output = new MROutput(outputContext, 2);
    output.initialize();

    assertEquals(false, output.isMapperOutput);
    assertEquals(false, output.useNewApi);
    assertEquals(org.apache.hadoop.mapred.TextOutputFormat.class, output.oldOutputFormat.getClass());
    assertNull(output.newOutputFormat);
    assertNotNull(output.oldApiTaskAttemptContext);
    assertNull(output.newApiTaskAttemptContext);
    assertNotNull(output.oldRecordWriter);
    assertNull(output.newRecordWriter);
    assertEquals(org.apache.hadoop.mapred.FileOutputCommitter.class, output.committer.getClass());
  }

  @Test(timeout = 5000)
  public void testNewAPI_SequenceFileOutputFormat() throws Exception {
    String outputPath = "/tmp/output";
    JobConf conf = new JobConf();
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);
    DataSinkDescriptor dataSink = MROutput
        .createConfigBuilder(conf, SequenceFileOutputFormat.class, outputPath)
        .build();

    OutputContext outputContext = createMockOutputContext(dataSink.getOutputDescriptor().getUserPayload());
    MROutput output = new MROutput(outputContext, 2);
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

  @Test(timeout = 5000)
  public void testOldAPI_SequenceFileOutputFormat() throws Exception {
    String outputPath = "/tmp/output";
    JobConf conf = new JobConf();
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);
    DataSinkDescriptor dataSink = MROutput
        .createConfigBuilder(conf, org.apache.hadoop.mapred.SequenceFileOutputFormat.class, outputPath)
        .build();

    OutputContext outputContext = createMockOutputContext(dataSink.getOutputDescriptor().getUserPayload());
    MROutput output = new MROutput(outputContext, 2);
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
