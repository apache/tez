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
package org.apache.tez.mapreduce.processor.map;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.hadoop.mapreduce.MapContextImpl;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutputLegacy;
import org.apache.tez.mapreduce.processor.MRTask;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;

@SuppressWarnings({ "unchecked", "rawtypes" })
@Private
public class MapProcessor extends MRTask{

  private static final Log LOG = LogFactory.getLog(MapProcessor.class);

  public MapProcessor(ProcessorContext processorContext) {
    super(processorContext, true);
  }

  @Override
  public void handleEvents(List<Event> processorEvents) {
    // TODO Auto-generated method stub

  }

  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void run(Map<String, LogicalInput> inputs,
      Map<String, LogicalOutput> outputs) throws Exception {

    LOG.info("Running map: " + processorContext.getUniqueIdentifier());
    for (LogicalInput input : inputs.values()) {
      input.start();
    }
    for (LogicalOutput output : outputs.values()) {
      output.start();
    }

    if (inputs.size() != 1
        || outputs.size() != 1) {
      throw new IOException("Cannot handle multiple inputs or outputs"
          + ", inputCount=" + inputs.size()
          + ", outputCount=" + outputs.size());
    }
    LogicalInput in = inputs.values().iterator().next();
    LogicalOutput out = outputs.values().iterator().next();

    initTask(out);

    // Sanity check
    if (!(in instanceof MRInputLegacy)) {
      throw new IOException(new TezException(
          "Only MRInputLegacy supported. Input: " + in.getClass()));
    }
    MRInputLegacy input = (MRInputLegacy)in;
    input.init();
    Configuration incrementalConf = input.getConfigUpdates();
    if (incrementalConf != null) {
      for (Entry<String, String> entry : incrementalConf) {
        jobConf.set(entry.getKey(), entry.getValue());
      }
    }

    KeyValueWriter kvWriter = null;
    if ((out instanceof MROutputLegacy)) {
      kvWriter = ((MROutputLegacy)out).getWriter();
    } else if ((out instanceof OrderedPartitionedKVOutput)){
      kvWriter = ((OrderedPartitionedKVOutput)out).getWriter();
    } else {
      throw new IOException("Illegal output to map, outputClass="
          + out.getClass());
    }

    if (useNewApi) {
      runNewMapper(jobConf, mrReporter, input, kvWriter);
    } else {
      runOldMapper(jobConf, mrReporter, input, kvWriter);
    }

    done();
  }
  

  /**
   * Update the job with details about the file split
   * @param job the job configuration to update
   * @param inputSplit the file split
   */
  private void updateJobWithSplit(final JobConf job, InputSplit inputSplit) {
    if (inputSplit instanceof FileSplit) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      job.set(JobContext.MAP_INPUT_FILE, fileSplit.getPath().toString());
      job.setLong(JobContext.MAP_INPUT_START, fileSplit.getStart());
      job.setLong(JobContext.MAP_INPUT_PATH, fileSplit.getLength());
    }
    LOG.info("Processing mapred split: " + inputSplit);
  }
  
  private void updateJobWithSplit(
      final JobConf job, org.apache.hadoop.mapreduce.InputSplit inputSplit) {
    if (inputSplit instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
      org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit = 
          (org.apache.hadoop.mapreduce.lib.input.FileSplit) inputSplit;
      job.set(JobContext.MAP_INPUT_FILE, fileSplit.getPath().toString());
      job.setLong(JobContext.MAP_INPUT_START, fileSplit.getStart());
      job.setLong(JobContext.MAP_INPUT_PATH, fileSplit.getLength());
    }
    LOG.info("Processing mapreduce split: " + inputSplit);
  }

  void runOldMapper(
      final JobConf job,
      final MRTaskReporter reporter,
      final MRInputLegacy input,
      final KeyValueWriter output
      ) throws IOException, InterruptedException {

    // Initialize input in-line since it sets parameters which may be used by the processor.
    // Done only for MRInput.
    // TODO use new method in MRInput to get required info
    //input.initialize(job, master);
    
    InputSplit inputSplit = input.getOldInputSplit();
    
    updateJobWithSplit(job, inputSplit);

    RecordReader in = new OldRecordReader(input);

    OutputCollector collector = new OldOutputCollector(output);

    MapRunnable runner =
        (MapRunnable)ReflectionUtils.newInstance(job.getMapRunnerClass(), job);

    runner.run(in, collector, (Reporter)reporter);
    
    // Set progress to 1.0f if there was no exception,
    reporter.setProgress(1.0f);
    // start the sort phase only if there are reducers
    this.statusUpdate();
  }

  private void runNewMapper(final JobConf job,
      MRTaskReporter reporter,
      final MRInputLegacy in,
      KeyValueWriter out
      ) throws IOException, InterruptedException {

    // Initialize input in-line since it sets parameters which may be used by the processor.
    // Done only for MRInput.
    // TODO use new method in MRInput to get required info
    //in.initialize(job, master);

    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
        getTaskAttemptContext();

    // make a mapper
    org.apache.hadoop.mapreduce.Mapper mapper;
    try {
      mapper = (org.apache.hadoop.mapreduce.Mapper)
          ReflectionUtils.newInstance(taskContext.getMapperClass(), job);
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    }

    org.apache.hadoop.mapreduce.RecordReader input =
        new NewRecordReader(in);

    org.apache.hadoop.mapreduce.RecordWriter output =
        new NewOutputCollector(out);

    org.apache.hadoop.mapreduce.InputSplit split = in.getNewInputSplit();
    
    updateJobWithSplit(job, split);

    org.apache.hadoop.mapreduce.MapContext
    mapContext =
    new MapContextImpl(
        job, taskAttemptId,
        input, output,
        committer,
        processorContext, split, reporter);

    org.apache.hadoop.mapreduce.Mapper.Context mapperContext =
        new WrappedMapper().getMapContext(mapContext);

    input.initialize(split, mapperContext);
    mapper.run(mapperContext);
    // Set progress to 1.0f if there was no exception,
    reporter.setProgress(1.0f);
    
    this.statusUpdate();
    input.close();
    output.close(mapperContext);
  }

  private static class NewRecordReader extends
      org.apache.hadoop.mapreduce.RecordReader {
    private final MRInput in;
    private KeyValueReader reader;

    private NewRecordReader(MRInput in) throws IOException {
      this.in = in;
      this.reader = in.getReader();
    }

    @Override
    public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
        TaskAttemptContext context) throws IOException,
        InterruptedException {
      //in.initializeNewRecordReader(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException,
        InterruptedException {
      return reader.next();
    }

    @Override
    public Object getCurrentKey() throws IOException,
        InterruptedException {
      return reader.getCurrentKey();
    }

    @Override
    public Object getCurrentValue() throws IOException,
        InterruptedException {
      return reader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return in.getProgress();
    }

    @Override
    public void close() throws IOException {
    }
  }

  private static class OldRecordReader implements RecordReader {
    private final MRInputLegacy mrInput;

    private OldRecordReader(MRInputLegacy mrInput) {
      this.mrInput = mrInput;
    }

    @Override
    public boolean next(Object key, Object value) throws IOException {
      // TODO broken
//      mrInput.setKey(key);
//      mrInput.setValue(value);
//      try {
//        return mrInput.hasNext();
//      } catch (InterruptedException ie) {
//        throw new IOException(ie);
//      }
      return mrInput.getOldRecordReader().next(key, value);
    }

    @Override
    public Object createKey() {
      return mrInput.getOldRecordReader().createKey();
    }

    @Override
    public Object createValue() {
      return mrInput.getOldRecordReader().createValue();
    }

    @Override
    public long getPos() throws IOException {
      return mrInput.getOldRecordReader().getPos();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
      try {
        return mrInput.getProgress();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
  }

  private static class OldOutputCollector
  implements OutputCollector {
    private final KeyValueWriter output;

    OldOutputCollector(KeyValueWriter output) {
      this.output = output;
    }

    public void collect(Object key, Object value) throws IOException {
        output.write(key, value);
    }
  }

  private class NewOutputCollector
    extends org.apache.hadoop.mapreduce.RecordWriter {
    private final KeyValueWriter out;

    NewOutputCollector(KeyValueWriter out) throws IOException {
      this.out = out;
    }

    @Override
    public void write(Object key, Object value) throws IOException, InterruptedException {
      out.write(key, value);
    }

    @Override
    public void close(TaskAttemptContext context
                      ) throws IOException, InterruptedException {
    }
  }

  @Override
  public void localizeConfiguration(JobConf jobConf)
      throws IOException, InterruptedException {
    super.localizeConfiguration(jobConf);
    jobConf.setBoolean(JobContext.TASK_ISMAP, true);
  }
}
