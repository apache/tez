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
package org.apache.tez.mapreduce.input;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReaderTez;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.mapreduce.processor.MRTask;
import org.apache.tez.mapreduce.processor.MRTaskReporter;

/**
 * {@link SimpleInput} is an {@link Input} which provides key/values pairs
 * for the consumer.
 *
 * It is compatible with all standard Apache Hadoop MapReduce 
 * {@link InputFormat} implementations.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class SimpleInput implements Input {

  private static final Log LOG = LogFactory.getLog(SimpleInput.class);
  
  MRTask task;
  
  boolean useNewApi;
  
  JobConf jobConf;
  
  org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext;

  org.apache.hadoop.mapreduce.InputFormat newInputFormat;
  org.apache.hadoop.mapreduce.RecordReader newRecordReader;
  
  org.apache.hadoop.mapred.InputFormat oldInputFormat;
  org.apache.hadoop.mapred.RecordReader oldRecordReader;

  protected TaskSplitIndex splitMetaInfo = new TaskSplitIndex();
  
  Object key;
  Object value;
  
  private TezCounter inputRecordCounter;
  private TezCounter fileInputByteCounter; 
  private List<Statistics> fsStats;
  private MRTaskReporter reporter;

  public SimpleInput(TezEngineTaskContext task, int index)
  {}
  
  public void setTask(MRTask task) {
    this.task = task;
  }

  public void initialize(Configuration conf, Master master) throws IOException,
      InterruptedException {
    if (task == null) {
      return;
    }
    
    if (conf instanceof JobConf) {
      jobConf = (JobConf)conf;
    } else {
      jobConf = new JobConf(conf);
    }
    
    // Read split information.
    TaskSplitMetaInfo[] allMetaInfo = readSplits(jobConf);
    TaskSplitMetaInfo thisTaskMetaInfo = allMetaInfo[task.getTaskAttemptId()
        .getTaskID().getId()];
    splitMetaInfo = new TaskSplitIndex(thisTaskMetaInfo.getSplitLocation(),
        thisTaskMetaInfo.getStartOffset());
    
    
    useNewApi = jobConf.getUseNewMapper();
    taskAttemptContext = task.getTaskAttemptContext();
    
    inputRecordCounter = task.getInputRecordsCounter();
    fileInputByteCounter = task.getFileInputBytesCounter();

    reporter = task.getMRReporter();

    if (useNewApi) {
      try {
        newInputFormat = 
            ReflectionUtils.newInstance(
                taskAttemptContext.getInputFormatClass(), jobConf);
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }
      
      newInputSplit = getNewSplitDetails(splitMetaInfo);
      List<Statistics> matchedStats = null;
      if (newInputSplit instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
        matchedStats = MRTask.getFsStatistics(
            ((org.apache.hadoop.mapreduce.lib.input.FileSplit)
                newInputSplit).getPath(), jobConf);
      }
      fsStats = matchedStats;
      newRecordReader = 
          newInputFormat.createRecordReader(newInputSplit, taskAttemptContext);
    } else {
      oldInputFormat = jobConf.getInputFormat();
      org.apache.hadoop.mapred.InputSplit oldInputSplit =
          getOldSplitDetails(splitMetaInfo);
      
      List<Statistics> matchedStats = null;
      if (oldInputSplit instanceof FileSplit) {
        matchedStats = 
            MRTask.getFsStatistics(
                ((FileSplit)oldInputSplit).getPath(), jobConf);
      }
      fsStats = matchedStats;

      long bytesInPrev = getInputBytes();
      oldRecordReader = 
          jobConf.getInputFormat().getRecordReader(
              oldInputSplit, jobConf, reporter);
      long bytesInCurr = getInputBytes();
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);

      updateJobWithSplit(jobConf, oldInputSplit);
    }
  }

  public boolean hasNext() throws IOException, InterruptedException {
    boolean hasNext = false;
    long bytesInPrev = getInputBytes();

    if (useNewApi) { 
        hasNext = newRecordReader.nextKeyValue();
    } else {
      hasNext = oldRecordReader.next(key, value);
    }
    
    long bytesInCurr = getInputBytes();
    fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    reporter.setProgress(getProgress());

    if (hasNext) {
      inputRecordCounter.increment(1);
    }
    
    return hasNext;
  }

  private SimpleValueIterator vIter = new SimpleValueIterator();
  private SimpleIterable valuesIterable =
      new SimpleIterable(vIter);

  private org.apache.hadoop.mapreduce.InputSplit newInputSplit;

  public void setKey(Object key) {
    this.key = key;
  }
  
  public void setValue(Object value) {
    this.value = value;
  }

  public Object getNextKey() throws IOException, InterruptedException {
    if (useNewApi) {
      return newRecordReader.getCurrentKey();
    } else {
      return key;
    }
  }

  public Iterable getNextValues() throws IOException,
      InterruptedException {
    value = newRecordReader.getCurrentValue();
    vIter.setValue(value);
    return valuesIterable;
  }

  public float getProgress() throws IOException, InterruptedException {
    if (useNewApi) {
      return newRecordReader.getProgress();
    } else {
      return oldRecordReader.getProgress();
    }
  }

  public void close() throws IOException {
    long bytesInPrev = getInputBytes();
    if (useNewApi) {
      newRecordReader.close();
    } else {
      oldRecordReader.close();
    }
    long bytesInCurr = getInputBytes();
    fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
  }

  static class SimpleValueIterator implements Iterator {

    private Object value;
    
    public void setValue(Object value) {
      this.value = value;
    }
    
    public boolean hasNext() {
      return false;
    }

    public Object next() {
      Object value = this.value;
      this.value = null;
      return value;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  static class SimpleIterable implements Iterable {
    private final Iterator iterator;
    public SimpleIterable(Iterator iterator) {
      this.iterator = iterator;
    }
    
    public Iterator iterator() {
      return iterator;
    }
  }
  

  public RecordReader getOldRecordReader() {
    return oldRecordReader;
  }
  
  public org.apache.hadoop.mapreduce.RecordReader getNewRecordReader() {
    return newRecordReader;
  }
  
  public org.apache.hadoop.mapred.InputSplit 
  getOldSplitDetails(TaskSplitIndex splitMetaInfo) 
      throws IOException {
    Path file = new Path(splitMetaInfo.getSplitLocation());
    FileSystem fs = FileSystem.getLocal(jobConf);
    file = fs.makeQualified(file);
    LOG.info("Reading input split file from : " + file);
    long offset = splitMetaInfo.getStartOffset();
    
    FSDataInputStream inFile = fs.open(file);
    inFile.seek(offset);
    String className = Text.readString(inFile);
    Class<org.apache.hadoop.mapred.InputSplit> cls;
    try {
      cls = 
          (Class<org.apache.hadoop.mapred.InputSplit>) 
          jobConf.getClassByName(className);
    } catch (ClassNotFoundException ce) {
      IOException wrap = new IOException("Split class " + className + 
          " not found");
      wrap.initCause(ce);
      throw wrap;
    }
    SerializationFactory factory = new SerializationFactory(jobConf);
    Deserializer<org.apache.hadoop.mapred.InputSplit> deserializer = 
        (Deserializer<org.apache.hadoop.mapred.InputSplit>) 
        factory.getDeserializer(cls);
    deserializer.open(inFile);
    org.apache.hadoop.mapred.InputSplit split = deserializer.deserialize(null);
    long pos = inFile.getPos();
    reporter.getCounter(TaskCounter.SPLIT_RAW_BYTES).increment(pos - offset);
    inFile.close();
    return split;
  }

  public org.apache.hadoop.mapreduce.InputSplit 
  getNewSplitDetails(TaskSplitIndex splitMetaInfo) 
      throws IOException {
    Path file = new Path(splitMetaInfo.getSplitLocation());
    long offset = splitMetaInfo.getStartOffset();
    
    // Split information read from local filesystem.
    FileSystem fs = FileSystem.getLocal(jobConf);
    file = fs.makeQualified(file);
    LOG.info("Reading input split file from : " + file);
    FSDataInputStream inFile = fs.open(file);
    inFile.seek(offset);
    String className = Text.readString(inFile);
    Class<org.apache.hadoop.mapreduce.InputSplit> cls;
    try {
      cls = 
          (Class<org.apache.hadoop.mapreduce.InputSplit>) 
          jobConf.getClassByName(className);
    } catch (ClassNotFoundException ce) {
      IOException wrap = new IOException("Split class " + className + 
          " not found");
      wrap.initCause(ce);
      throw wrap;
    }
    SerializationFactory factory = new SerializationFactory(jobConf);
    Deserializer<org.apache.hadoop.mapreduce.InputSplit> deserializer = 
        (Deserializer<org.apache.hadoop.mapreduce.InputSplit>) 
        factory.getDeserializer(cls);
    deserializer.open(inFile);
    org.apache.hadoop.mapreduce.InputSplit split = 
        deserializer.deserialize(null);
    long pos = inFile.getPos();
    reporter.getCounter(TaskCounter.SPLIT_RAW_BYTES).increment(pos - offset);
    inFile.close();
    return split;
  }

  private void updateJobWithSplit(final JobConf job, InputSplit inputSplit) {
    if (inputSplit instanceof FileSplit) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      job.set(JobContext.MAP_INPUT_FILE, fileSplit.getPath().toString());
      job.setLong(JobContext.MAP_INPUT_START, fileSplit.getStart());
      job.setLong(JobContext.MAP_INPUT_PATH, fileSplit.getLength());
    }
    LOG.info("Processing split: " + inputSplit);
  }

  private long getInputBytes() {
    if (fsStats == null) return 0;
    long bytesRead = 0;
    for (Statistics stat: fsStats) {
      bytesRead = bytesRead + stat.getBytesRead();
    }
    return bytesRead;
  }

  public void initializeNewRecordReader(
      org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context) 
  throws IOException, InterruptedException {
    newRecordReader.initialize(split, context);
  }
  
  public org.apache.hadoop.mapreduce.InputSplit getNewInputSplit() {
    return newInputSplit;
  }

  protected TaskSplitMetaInfo[] readSplits(Configuration conf)
      throws IOException {
    TaskSplitMetaInfo[] allTaskSplitMetaInfo;
    allTaskSplitMetaInfo = SplitMetaInfoReaderTez.readSplitMetaInfo(conf,
        FileSystem.getLocal(conf));
    return allTaskSplitMetaInfo;
  }
}
