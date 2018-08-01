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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.TezSharedExecutor;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.mapreduce.TestUmbilical;
import org.apache.tez.mapreduce.TezTestUtils;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;


public class TestMROutput {

  static File tmpDir;

  @BeforeClass
  public static void setupClass () {
    tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
  }

  @Test(timeout = 5000)
  public void testNewAPI_TextOutputFormat() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.IS_MAP_PROCESSOR, true);
    DataSinkDescriptor dataSink = MROutput
        .createConfigBuilder(conf, TextOutputFormat.class,
            tmpDir.getPath())
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
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.IS_MAP_PROCESSOR, false);
    DataSinkDescriptor dataSink = MROutput
        .createConfigBuilder(conf,
            org.apache.hadoop.mapred.TextOutputFormat.class,
            tmpDir.getPath())
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
    JobConf conf = new JobConf();
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);
    DataSinkDescriptor dataSink = MROutput
        .createConfigBuilder(conf, SequenceFileOutputFormat.class,
            tmpDir.getPath())
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
    JobConf conf = new JobConf();
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(Text.class);
    DataSinkDescriptor dataSink = MROutput
        .createConfigBuilder(conf,
            org.apache.hadoop.mapred.SequenceFileOutputFormat.class,
            tmpDir.getPath())
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

  // test to try and use the WorkOutputPathOutputFormat - this checks that the getDefaultWorkFile is
  // set while creating recordWriters
  @Test(timeout = 5000)
  public void testNewAPI_WorkOutputPathOutputFormat() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.IS_MAP_PROCESSOR, true);
    DataSinkDescriptor dataSink = MROutput
      .createConfigBuilder(conf, NewAPI_WorkOutputPathReadingOutputFormat.class,
          tmpDir.getPath())
      .build();

    OutputContext outputContext = createMockOutputContext(dataSink.getOutputDescriptor().getUserPayload());
    MROutput output = new MROutput(outputContext, 2);
    output.initialize();

    assertEquals(true, output.isMapperOutput);
    assertEquals(true, output.useNewApi);
    assertEquals(NewAPI_WorkOutputPathReadingOutputFormat.class, output.newOutputFormat.getClass());
    assertNull(output.oldOutputFormat);
    assertNotNull(output.newApiTaskAttemptContext);
    assertNull(output.oldApiTaskAttemptContext);
    assertNotNull(output.newRecordWriter);
    assertNull(output.oldRecordWriter);
    assertEquals(FileOutputCommitter.class, output.committer.getClass());
  }

  // test to try and use the WorkOutputPathOutputFormat - this checks that the workOutput path is
  // set while creating recordWriters
  @Test(timeout = 5000)
  public void testOldAPI_WorkOutputPathOutputFormat() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(MRConfig.IS_MAP_PROCESSOR, false);
    DataSinkDescriptor dataSink = MROutput
      .createConfigBuilder(conf, OldAPI_WorkOutputPathReadingOutputFormat.class,
          tmpDir.getPath())
      .build();

    OutputContext outputContext = createMockOutputContext(dataSink.getOutputDescriptor().getUserPayload());
    MROutput output = new MROutput(outputContext, 2);
    output.initialize();

    assertEquals(false, output.isMapperOutput);
    assertEquals(false, output.useNewApi);
    assertEquals(OldAPI_WorkOutputPathReadingOutputFormat.class, output.oldOutputFormat.getClass());
    assertNull(output.newOutputFormat);
    assertNotNull(output.oldApiTaskAttemptContext);
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
  
  public static LogicalIOProcessorRuntimeTask createLogicalTask(
      Configuration conf,
      TezUmbilical umbilical, String dagName,
      String vertexName, TezExecutors sharedExecutor) throws Exception {
    ProcessorDescriptor procDesc = ProcessorDescriptor.create(TestProcessor.class.getName());
    List<InputSpec> inputSpecs = Lists.newLinkedList();
    List<OutputSpec> outputSpecs = Lists.newLinkedList();
    outputSpecs.add(new OutputSpec("Null",
        MROutput.createConfigBuilder(conf, TestOutputFormat.class).build().getOutputDescriptor(), 1));
    
    TaskSpec taskSpec = new TaskSpec(
        TezTestUtils.getMockTaskAttemptId(0, 0, 0, 0),
        dagName, vertexName, -1,
        procDesc,
        inputSpecs,
        outputSpecs, null, null);

    FileSystem fs = FileSystem.getLocal(conf);
    Path workDir =
        new Path(new Path(System.getProperty("test.build.data", "/tmp")),
                 "TestMapOutput").makeQualified(fs.getUri(), fs.getWorkingDirectory());

    return new LogicalIOProcessorRuntimeTask(
        taskSpec,
        0,
        conf,
        new String[] {workDir.toString()},
        umbilical,
        null,
        new HashMap<String, String>(),
        HashMultimap.<String, String>create(), null, "", new ExecutionContextImpl("localhost"),
        Runtime.getRuntime().maxMemory(), true, new DefaultHadoopShim(), sharedExecutor);
  }

  public static class TestOutputCommitter extends OutputCommitter {

    @Override
    public void setupJob(JobContext jobContext) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
    }
    
  }
  
  public static class TestOutputFormat extends OutputFormat<String, String> {
    public static class TestRecordWriter extends RecordWriter<String, String> {
      Writer writer;
      boolean doWrite;
      TestRecordWriter(boolean write) throws IOException {
        this.doWrite = write;
        if (doWrite) {
          File f = File.createTempFile("test", null);
          f.deleteOnExit();
          writer = new BufferedWriter(new FileWriter(f));
        }
      }
      
      @Override
      public void write(String key, String value) throws IOException, InterruptedException {
        if (doWrite) {
          writer.write(key);
          writer.write(value);
        }
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        writer.close();
      }
      
    }
    
    @Override
    public RecordWriter<String, String> getRecordWriter(TaskAttemptContext context)
        throws IOException, InterruptedException {
      return new TestRecordWriter(true);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
        throws IOException, InterruptedException {
      return new TestOutputCommitter();
    }
  }

  // OldAPI OutputFormat class that reads the workoutput path while creating recordWriters
  public static class OldAPI_WorkOutputPathReadingOutputFormat extends org.apache.hadoop.mapred.FileOutputFormat<String, String> {
    public static class NoOpRecordWriter implements org.apache.hadoop.mapred.RecordWriter<String, String> {
      @Override
      public void write(String key, String value) throws IOException {}

      @Override
      public void close(Reporter reporter) throws IOException {}
    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<String, String> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
      // check work output path is not null
      Path workOutputPath = org.apache.hadoop.mapred.FileOutputFormat.getWorkOutputPath(job);
      assertNotNull(workOutputPath);
      return new NoOpRecordWriter();
    }
  }

  // NewAPI OutputFormat class that reads the default work file while creating recordWriters
  public static class NewAPI_WorkOutputPathReadingOutputFormat extends FileOutputFormat<String, String> {
    public static class NoOpRecordWriter extends RecordWriter<String, String> {
      @Override
      public void write(String key, String value) throws IOException, InterruptedException {
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      }
    }

    @Override
    public RecordWriter<String, String> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
      // check default work file is not null
      Path workOutputPath = getDefaultWorkFile(job, ".foo");
      assertNotNull(workOutputPath);
      return new NoOpRecordWriter();
    }
  }

  public static class TestProcessor extends SimpleProcessor {
    public TestProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void run() throws Exception {
      KeyValueWriter writer = (KeyValueWriter) getOutputs().values().iterator().next().getWriter();
      for (int i=0; i<1000000; ++i) {
        writer.write("key", "value");
      }
    }

  }

  @Ignore
  @Test
  public void testPerf() throws Exception {
    Configuration conf = new Configuration();
    TezSharedExecutor sharedExecutor = new TezSharedExecutor(conf);
    LogicalIOProcessorRuntimeTask task = createLogicalTask(conf, new TestUmbilical(), "dag",
        "vertex", sharedExecutor);
    task.initialize();
    task.run();
    task.close();
    sharedExecutor.shutdownNow();
  }
}
