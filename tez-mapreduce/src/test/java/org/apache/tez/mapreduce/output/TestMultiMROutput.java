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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.OutputStatisticsReporter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;


public class TestMultiMROutput {

  @Test(timeout = 5000)
  public void testNewAPI_TextOutputFormat() throws Exception {
    validate(true, TextOutputFormat.class, true, FileOutputCommitter.class,
        false);
  }

  @Test(timeout = 5000)
  public void testOldAPI_TextOutputFormat() throws Exception {
    validate(false, org.apache.hadoop.mapred.TextOutputFormat.class, false,
        org.apache.hadoop.mapred.FileOutputCommitter.class, false);
  }

  @Test(timeout = 5000)
  public void testNewAPI_SequenceFileOutputFormat() throws Exception {
    validate(true, SequenceFileOutputFormat.class, false,
        FileOutputCommitter.class, false);
  }

  @Test(timeout = 5000)
  public void testOldAPI_SequenceFileOutputFormat() throws Exception {
    validate(false, org.apache.hadoop.mapred.SequenceFileOutputFormat.class,
        false, org.apache.hadoop.mapred.FileOutputCommitter.class, false);
  }

  @Test(timeout = 5000)
  public void testNewAPI_LazySequenceFileOutputFormat() throws Exception {
    validate(true, SequenceFileOutputFormat.class, false,
        FileOutputCommitter.class, true);
  }

  @Test(timeout = 5000)
  public void testOldAPI_LazySequenceFileOutputFormat() throws Exception {
    validate(false, org.apache.hadoop.mapred.SequenceFileOutputFormat.class,
        false, org.apache.hadoop.mapred.FileOutputCommitter.class, true);
  }

  @Test(timeout = 5000)
  public void testNewAPI_LazyTextOutputFormat() throws Exception {
    validate(true, TextOutputFormat.class, false,
        FileOutputCommitter.class, true);
  }

  @Test(timeout = 5000)
  public void testOldAPI_LazyTextOutputFormat() throws Exception {
    validate(false, org.apache.hadoop.mapred.TextOutputFormat.class, false,
        org.apache.hadoop.mapred.FileOutputCommitter.class, true);
  }

  @Test(timeout = 5000)
  public void testInvalidBasePath() throws Exception {
    MultiMROutput outputs = createMROutputs(SequenceFileOutputFormat.class,
        false, true);
    try {
      outputs.getWriter().write(new Text(Integer.toString(0)),
          new Text("foo"), "/tmp");
      Assert.assertTrue(false); // should not come here
    } catch (UnsupportedOperationException uoe) {
    }
  }

  private OutputContext createMockOutputContext(UserPayload payload) {
    OutputContext outputContext = mock(OutputContext.class);
    ApplicationId appId = ApplicationId.newInstance(System.currentTimeMillis(), 1);
    when(outputContext.getUserPayload()).thenReturn(payload);
    when(outputContext.getApplicationId()).thenReturn(appId);
    when(outputContext.getTaskVertexIndex()).thenReturn(1);
    when(outputContext.getTaskAttemptNumber()).thenReturn(1);
    when(outputContext.getCounters()).thenReturn(new TezCounters());
    when(outputContext.getStatisticsReporter()).thenReturn(
        mock(OutputStatisticsReporter.class));
    return outputContext;
  }

  private void validate(boolean expectedUseNewAPIValue, Class outputFormat,
      boolean isMapper, Class committerClass, boolean useLazyOutputFormat)
          throws InterruptedException, IOException {
    MultiMROutput output = createMROutputs(outputFormat, isMapper,
        useLazyOutputFormat);

    assertEquals(isMapper, output.isMapperOutput);
    assertEquals(expectedUseNewAPIValue, output.useNewApi);
    if (expectedUseNewAPIValue) {
      if (useLazyOutputFormat) {
        assertEquals(LazyOutputFormat.class,
            output.newOutputFormat.getClass());
      } else {
        assertEquals(outputFormat, output.newOutputFormat.getClass());
      }
      assertNotNull(output.newApiTaskAttemptContext);
      assertNull(output.oldOutputFormat);
      assertEquals(Text.class,
          output.newApiTaskAttemptContext.getOutputValueClass());
      assertEquals(Text.class,
          output.newApiTaskAttemptContext.getOutputKeyClass());
      assertNull(output.oldApiTaskAttemptContext);
      assertNotNull(output.newRecordWriters);
      assertNull(output.oldRecordWriters);
    } else {
      if (!useLazyOutputFormat) {
        assertEquals(outputFormat, output.oldOutputFormat.getClass());
      } else {
        assertEquals(org.apache.hadoop.mapred.lib.LazyOutputFormat.class,
            output.oldOutputFormat.getClass());
      }
      assertNull(output.newOutputFormat);
      assertNotNull(output.oldApiTaskAttemptContext);
      assertNull(output.newApiTaskAttemptContext);
      assertEquals(Text.class,
          output.oldApiTaskAttemptContext.getOutputValueClass());
      assertEquals(Text.class,
          output.oldApiTaskAttemptContext.getOutputKeyClass());
      assertNotNull(output.oldRecordWriters);
      assertNull(output.newRecordWriters);
    }

    assertEquals(committerClass, output.committer.getClass());
    int numOfUniqueKeys = 3;
    for (int i=0; i<numOfUniqueKeys; i++) {
      output.getWriter().write(new Text(Integer.toString(i)),
          new Text("foo"), Integer.toString(i));
    }
    output.close();
    if (expectedUseNewAPIValue) {
      assertEquals(numOfUniqueKeys, output.newRecordWriters.size());
    } else {
      assertEquals(numOfUniqueKeys, output.oldRecordWriters.size());
    }
  }

  private MultiMROutput createMROutputs(Class outputFormat,
      boolean isMapper, boolean useLazyOutputFormat)
          throws InterruptedException, IOException {
    String outputPath = "/tmp/output";
    JobConf conf = new JobConf();
    conf.setBoolean(MRConfig.IS_MAP_PROCESSOR, isMapper);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    DataSinkDescriptor dataSink = MultiMROutput.createConfigBuilder(
        conf, outputFormat, outputPath, useLazyOutputFormat).build();

    OutputContext outputContext = createMockOutputContext(
        dataSink.getOutputDescriptor().getUserPayload());
    MultiMROutput output = new MultiMROutput(outputContext, 2);
    output.initialize();
    return output;
  }
}
