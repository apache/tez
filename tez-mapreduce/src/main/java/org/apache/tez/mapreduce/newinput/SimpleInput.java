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
package org.apache.tez.mapreduce.newinput;

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
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReaderTez;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.KVReader;
import org.apache.tez.engine.newapi.LogicalInput;
import org.apache.tez.engine.newapi.TezInputContext;
import org.apache.tez.mapreduce.common.Utils;
import org.apache.tez.mapreduce.hadoop.newmapred.MRReporter;
import org.apache.tez.mapreduce.hadoop.newmapreduce.TaskAttemptContextImpl;

/**
 * {@link SimpleInput} is an {@link Input} which provides key/values pairs
 * for the consumer.
 *
 * It is compatible with all standard Apache Hadoop MapReduce 
 * {@link InputFormat} implementations.
 */

public class SimpleInput implements LogicalInput {

  private static final Log LOG = LogFactory.getLog(SimpleInput.class);
  
  
  private TezInputContext inputContext;
  
  private JobConf jobConf;
  private Configuration incrementalConf;
  
  boolean useNewApi;
  
  org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext;

  @SuppressWarnings("rawtypes")
  private org.apache.hadoop.mapreduce.InputFormat newInputFormat;
  @SuppressWarnings("rawtypes")
  private org.apache.hadoop.mapreduce.RecordReader newRecordReader;
  private org.apache.hadoop.mapreduce.InputSplit newInputSplit;
  
  @SuppressWarnings("rawtypes")
  private InputFormat oldInputFormat;
  @SuppressWarnings("rawtypes")
  private RecordReader oldRecordReader;

  protected TaskSplitIndex splitMetaInfo = new TaskSplitIndex();
  
  // Setup the values iterator once, and set value on the same object each time
  // to prevent lots of objects being created.
  private SimpleValueIterator valueIterator = new SimpleValueIterator();
  private SimpleIterable valueIterable = new SimpleIterable(valueIterator);

  private TezCounter inputRecordCounter;
  private TezCounter fileInputByteCounter; 
  private List<Statistics> fsStats;

  @Override
  public List<Event> initialize(TezInputContext inputContext) throws IOException {
    Configuration conf = TezUtils.createConfFromUserPayload(inputContext.getUserPayload());
    this.jobConf = new JobConf(conf);
    
 // Read split information.
    TaskSplitMetaInfo[] allMetaInfo = readSplits(conf);
    TaskSplitMetaInfo thisTaskMetaInfo = allMetaInfo[inputContext.getTaskIndex()];
    this.splitMetaInfo = new TaskSplitIndex(thisTaskMetaInfo.getSplitLocation(),
        thisTaskMetaInfo.getStartOffset());
    
    // TODO NEWTEZ Rename this to be specific to SimpleInput. This Input, in
    // theory, can be used by the MapProcessor, ReduceProcessor or a custom
    // processor. (The processor could provide the counter though)
    this.inputRecordCounter = inputContext.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS);
    this.fileInputByteCounter = inputContext.getCounters().findCounter(FileInputFormatCounter.BYTES_READ);
    
    useNewApi = this.jobConf.getUseNewMapper();

    if (useNewApi) {
      TaskAttemptContext taskAttemptContext = createTaskAttemptContext();
      Class<? extends org.apache.hadoop.mapreduce.InputFormat<?, ?>> inputFormatClazz;
      try {
        inputFormatClazz = taskAttemptContext.getInputFormatClass();
      } catch (ClassNotFoundException e) {
        throw new IOException("Unable to instantiate InputFormat class", e);
      }

      newInputFormat = ReflectionUtils.newInstance(inputFormatClazz, this.jobConf);

      newInputSplit = getNewSplitDetails(splitMetaInfo);

      List<Statistics> matchedStats = null;
      if (newInputSplit instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
        matchedStats = Utils.getFsStatistics(
            ((org.apache.hadoop.mapreduce.lib.input.FileSplit)
                newInputSplit).getPath(), this.jobConf);
      }
      fsStats = matchedStats;
      
      try {
        newRecordReader = newInputFormat.createRecordReader(newInputSplit, taskAttemptContext);
        newRecordReader.initialize(newInputSplit, taskAttemptContext);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while creating record reader", e);
      }
    } else { // OLD API
      oldInputFormat = this.jobConf.getInputFormat();
      InputSplit oldInputSplit =
          getOldSplitDetails(splitMetaInfo);
      
      
      List<Statistics> matchedStats = null;
      if (oldInputSplit instanceof FileSplit) {
        matchedStats = Utils.getFsStatistics(((FileSplit) oldInputSplit).getPath(), this.jobConf);
      }
      fsStats = matchedStats;
      
      long bytesInPrev = getInputBytes();
      oldRecordReader = oldInputFormat.getRecordReader(oldInputSplit,
          this.jobConf, new MRReporter(inputContext, oldInputSplit));
      long bytesInCurr = getInputBytes();
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
      setIncrementalConfigParams(oldInputSplit);
    }    
    return null;
  }

  @Override
  public KVReader getReader() throws IOException {
    return new KVReader() {
      
      Object key;
      Object value;
      
      private final boolean localNewApi = useNewApi;
      
      @SuppressWarnings("unchecked")
      @Override
      public boolean next() throws IOException {
        boolean hasNext = false;
        long bytesInPrev = getInputBytes();
        if (localNewApi) {
          try {
            hasNext = newRecordReader.nextKeyValue();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while checking for next key-value", e);
          }
        } else {
          hasNext = oldRecordReader.next(key, value);
        }
        long bytesInCurr = getInputBytes();
        fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
        
        if (hasNext) {
          inputRecordCounter.increment(1);
        }
        
        return hasNext;
      }

      @Override
      public KVRecord getCurrentKV() throws IOException {
        KVRecord kvRecord = null;
        if (localNewApi) {
          try {
            valueIterator.setValue(newRecordReader.getCurrentValue());
            kvRecord = new KVRecord(newRecordReader.getCurrentKey(), valueIterable);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while fetching next key-value", e);
          }
          
        } else {
          valueIterator.setValue(value);
          kvRecord = new KVRecord(key, valueIterable);
        }
        return kvRecord;
      }
    };
  }


  @Override
  public void handleEvents(List<Event> inputEvents) {
    // Not expecting any events at the moment.
  }


  @Override
  public void setNumPhysicalInputs(int numInputs) {
    // Not required at the moment. May be required if splits are sent via events.
  }

  public List<Event> close() throws IOException {
    long bytesInPrev = getInputBytes();
    if (useNewApi) {
      newRecordReader.close();
    } else {
      oldRecordReader.close();
    }
    long bytesInCurr = getInputBytes();
    fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    
    return null;
  }

  /**
   * SimpleInputs sets some additional parameters like split location when using
   * the new API. This methods returns the list of additional updates, and
   * should be used by Processors using the old MapReduce API with SimpleInput.
   * 
   * @return the additional fields set by SimpleInput
   */
  public Configuration getConfigUpdates() {
    return new Configuration(incrementalConf);
  }

  public float getProgress() throws IOException, InterruptedException {
    if (useNewApi) {
      return newRecordReader.getProgress();
    } else {
      return oldRecordReader.getProgress();
    }
  }

  
  private TaskAttemptContext createTaskAttemptContext() {
    return new TaskAttemptContextImpl(this.jobConf, inputContext, true);
  }
  

  private static class SimpleValueIterator implements Iterator<Object> {

    private Object value;
    int nextCount = 0;

    public void setValue(Object value) {
      this.value = value;
    }

    public boolean hasNext() {
      return nextCount == 0;
    }

    public Object next() {
      nextCount++;
      Object value = this.value;
      this.value = null;
      return value;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static class SimpleIterable implements Iterable<Object> {
    private final Iterator<Object> iterator;
    public SimpleIterable(Iterator<Object> iterator) {
      this.iterator = iterator;
    }

    @Override
    public Iterator<Object> iterator() {
      return iterator;
    }
  }



  
  @SuppressWarnings("unchecked")
  private InputSplit getOldSplitDetails(TaskSplitIndex splitMetaInfo)
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
    inputContext.getCounters().findCounter(TaskCounter.SPLIT_RAW_BYTES)
        .increment(pos - offset);
    inFile.close();
    return split;
  }

  @SuppressWarnings("unchecked")
  private org.apache.hadoop.mapreduce.InputSplit getNewSplitDetails(
      TaskSplitIndex splitMetaInfo) throws IOException {
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
    inputContext.getCounters().findCounter(TaskCounter.SPLIT_RAW_BYTES)
        .increment(pos - offset);
    inFile.close();
    return split;
  }

  private void setIncrementalConfigParams(InputSplit inputSplit) {
    if (inputSplit instanceof FileSplit) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      this.incrementalConf = new Configuration(false);

      this.incrementalConf.set(JobContext.MAP_INPUT_FILE, fileSplit.getPath()
          .toString());
      this.incrementalConf.setLong(JobContext.MAP_INPUT_START,
          fileSplit.getStart());
      this.incrementalConf.setLong(JobContext.MAP_INPUT_PATH,
          fileSplit.getLength());
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

  protected TaskSplitMetaInfo[] readSplits(Configuration conf)
      throws IOException {
    TaskSplitMetaInfo[] allTaskSplitMetaInfo;
    allTaskSplitMetaInfo = SplitMetaInfoReaderTez.readSplitMetaInfo(conf,
        FileSystem.getLocal(conf));
    return allTaskSplitMetaInfo;
  }
}
