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
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReaderTez;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.mapred.MRReporter;
import org.apache.tez.mapreduce.hadoop.mapreduce.TaskAttemptContextImpl;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.library.api.KeyValueReader;

import com.google.common.base.Preconditions;

/**
 * {@link MRInput} is an {@link Input} which provides key/values pairs
 * for the consumer.
 *
 * It is compatible with all standard Apache Hadoop MapReduce 
 * {@link InputFormat} implementations.
 * 
 * This class is not meant to be extended by external projects.
 */

public class MRInput implements LogicalInput {

  private static final Log LOG = LogFactory.getLog(MRInput.class);
  
  private final Lock rrLock = new ReentrantLock();
  private Condition rrInited = rrLock.newCondition();
  private TezInputContext inputContext;
  
  private volatile boolean eventReceived = false;
  
  private JobConf jobConf;
  private Configuration incrementalConf;
  private boolean readerCreated = false;
  
  boolean useNewApi;
  
  org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext;

  @SuppressWarnings("rawtypes")
  private org.apache.hadoop.mapreduce.InputFormat newInputFormat;
  @SuppressWarnings("rawtypes")
  private org.apache.hadoop.mapreduce.RecordReader newRecordReader;
  protected org.apache.hadoop.mapreduce.InputSplit newInputSplit;

  @SuppressWarnings("rawtypes")
  private InputFormat oldInputFormat;
  @SuppressWarnings("rawtypes")
  protected RecordReader oldRecordReader;
  protected InputSplit oldInputSplit;

  protected TaskSplitIndex splitMetaInfo = new TaskSplitIndex();
  
  private TezCounter inputRecordCounter;
  // Potential counters - #splits, #totalSize, #actualyBytesRead
  
  @Private
  volatile boolean splitInfoViaEvents;
  
  
  @Override
  public List<Event> initialize(TezInputContext inputContext) throws IOException {
    this.inputContext = inputContext;
    this.inputContext.requestInitialMemory(0l, null); //mandatory call
    this.inputContext.inputIsReady();
    MRInputUserPayloadProto mrUserPayload =
      MRHelpers.parseMRInputPayload(inputContext.getUserPayload());
    Preconditions.checkArgument(mrUserPayload.hasSplits() == false,
        "Split information not expected in MRInput");
    Configuration conf =
      MRHelpers.createConfFromByteString(mrUserPayload.getConfigurationBytes());
    this.jobConf = new JobConf(conf);
    // Add tokens to the jobConf - in case they are accessed within the RR / IF
    jobConf.getCredentials().mergeAll(UserGroupInformation.getCurrentUser().getCredentials());

    TaskAttemptID taskAttemptId = new TaskAttemptID(
      new TaskID(
        Long.toString(inputContext.getApplicationId().getClusterTimestamp()),
        inputContext.getApplicationId().getId(), TaskType.MAP,
        inputContext.getTaskIndex()),
      inputContext.getTaskAttemptNumber());

    jobConf.set(MRJobConfig.TASK_ATTEMPT_ID,
      taskAttemptId.toString());
    jobConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
      inputContext.getDAGAttemptNumber());

    // TODO NEWTEZ Rename this to be specific to MRInput. This Input, in
    // theory, can be used by the MapProcessor, ReduceProcessor or a custom
    // processor. (The processor could provide the counter though)

    this.inputRecordCounter = inputContext.getCounters().findCounter(TaskCounter.INPUT_RECORDS_PROCESSED);

    useNewApi = this.jobConf.getUseNewMapper();
    this.splitInfoViaEvents = jobConf.getBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS,
        MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS_DEFAULT);
    LOG.info("Using New mapreduce API: " + useNewApi
        + ", split information via event: " + splitInfoViaEvents);

    initializeInternal();
    return null;
  }
  
  @Override
  public void start() {
  }

  @Private
  void initializeInternal() throws IOException {
    // Primarily for visibility
    rrLock.lock();
    try {
      if (splitInfoViaEvents) {
        if (useNewApi) {
          setupNewInputFormat();
        } else {
          setupOldInputFormat();
        }
      } else {
        // Read split information.
        TaskSplitMetaInfo[] allMetaInfo = readSplits(jobConf);
        TaskSplitMetaInfo thisTaskMetaInfo = allMetaInfo[inputContext
            .getTaskIndex()];
        this.splitMetaInfo = new TaskSplitIndex(
            thisTaskMetaInfo.getSplitLocation(),
            thisTaskMetaInfo.getStartOffset());
        if (useNewApi) {
          setupNewInputFormat();
          newInputSplit = getNewSplitDetailsFromDisk(splitMetaInfo);
          setupNewRecordReader();
        } else {
          setupOldInputFormat();
          oldInputSplit = getOldSplitDetailsFromDisk(splitMetaInfo);
          setupOldRecordReader();
        }
      }
    } finally {
      rrLock.unlock();
    }
    LOG.info("Initialzed MRInput: " + inputContext.getSourceVertexName());
  }

  private void setupOldInputFormat() {
    oldInputFormat = this.jobConf.getInputFormat();
  }
  
  private void setupOldRecordReader() throws IOException {
    Preconditions.checkNotNull(oldInputSplit, "Input split hasn't yet been setup");
    oldRecordReader = oldInputFormat.getRecordReader(oldInputSplit,
        this.jobConf, new MRReporter(inputContext, oldInputSplit));
    setIncrementalConfigParams(oldInputSplit);
  }
  
  private void setupNewInputFormat() throws IOException {
    taskAttemptContext = createTaskAttemptContext();
    Class<? extends org.apache.hadoop.mapreduce.InputFormat<?, ?>> inputFormatClazz;
    try {
      inputFormatClazz = taskAttemptContext.getInputFormatClass();
    } catch (ClassNotFoundException e) {
      throw new IOException("Unable to instantiate InputFormat class", e);
    }

    newInputFormat = ReflectionUtils.newInstance(inputFormatClazz, this.jobConf);
  }
  
  private void setupNewRecordReader() throws IOException {
    Preconditions.checkNotNull(newInputSplit, "Input split hasn't yet been setup");    
    try {
      newRecordReader = newInputFormat.createRecordReader(newInputSplit, taskAttemptContext);
      newRecordReader.initialize(newInputSplit, taskAttemptContext);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while creating record reader", e);
    }
  }

  @Override
  public KeyValueReader getReader() throws IOException {
    Preconditions
        .checkState(readerCreated == false,
            "Only a single instance of record reader can be created for this input.");
    readerCreated = true;
    rrLock.lock();
    try {
      if (newRecordReader == null && oldRecordReader == null)
        checkAndAwaitRecordReaderInitialization();
    } finally {
      rrLock.unlock();
    }

    LOG.info("Creating reader for MRInput: "
        + inputContext.getSourceVertexName());
    return new MRInputKVReader();
  }

  @Override
  public void handleEvents(List<Event> inputEvents) throws Exception {
    if (eventReceived || inputEvents.size() != 1) {
      throw new IllegalStateException(
          "MRInput expects only a single input. Received: current eventListSize: "
              + inputEvents.size() + "Received previous input: "
              + eventReceived);
    }
    Event event = inputEvents.iterator().next();
    Preconditions.checkArgument(event instanceof RootInputDataInformationEvent,
        getClass().getSimpleName()
            + " can only handle a single event of type: "
            + RootInputDataInformationEvent.class.getSimpleName());

    processSplitEvent((RootInputDataInformationEvent)event);
  }

  

  @Override
  public void setNumPhysicalInputs(int numInputs) {
    // Not required at the moment. May be required if splits are sent via events.
  }

  @Override
  public List<Event> close() throws IOException {
    if (useNewApi) {
      newRecordReader.close();
    } else {
      oldRecordReader.close();
    }
    return null;
  }

  /**
   * {@link MRInput} sets some additional parameters like split location when using
   * the new API. This methods returns the list of additional updates, and
   * should be used by Processors using the old MapReduce API with {@link MRInput}.
   * 
   * @return the additional fields set by {@link MRInput}
   */
  public Configuration getConfigUpdates() {
    if (incrementalConf != null) {
      return new Configuration(incrementalConf);
    }
    return null;
  }
  
  

  public float getProgress() throws IOException, InterruptedException {
    if (useNewApi) {
      return newRecordReader.getProgress();
    } else {
      return oldRecordReader.getProgress();
    }
  }

  
  private TaskAttemptContext createTaskAttemptContext() {
    return new TaskAttemptContextImpl(this.jobConf, inputContext, true, null);
  }
  
  void processSplitEvent(RootInputDataInformationEvent event)
      throws IOException {
    rrLock.lock();
    try {
      initFromEventInternal(event);
      LOG.info("Notifying on RecordReader Initialized");
      rrInited.signal();
    } finally {
      rrLock.unlock();
    }
  }
  
  void checkAndAwaitRecordReaderInitialization() throws IOException {
    try {
      LOG.info("Awaiting RecordReader initialization");
      rrInited.await();
    } catch (Exception e) {
      throw new IOException(
          "Interrupted waiting for RecordReader initiailization");
    }
  }

  @Private
  void initFromEvent(RootInputDataInformationEvent initEvent)
      throws IOException {
    rrLock.lock();
    try {
      initFromEventInternal(initEvent);
    } finally {
      rrLock.unlock();
    }
  }
  
  private void initFromEventInternal(RootInputDataInformationEvent initEvent)
      throws IOException {
    LOG.info("Initializing RecordReader from event");
    Preconditions.checkState(initEvent != null, "InitEvent must be specified");
    MRSplitProto splitProto = MRSplitProto
        .parseFrom(initEvent.getUserPayload());
    if (useNewApi) {
      newInputSplit = getNewSplitDetailsFromEvent(splitProto, jobConf);
      LOG.info("Split Details -> SplitClass: "
          + newInputSplit.getClass().getName() + ", NewSplit: " + newInputSplit);
      setupNewRecordReader();
    } else {
      oldInputSplit = getOldSplitDetailsFromEvent(splitProto, jobConf);
      LOG.info("Split Details -> SplitClass: "
          + oldInputSplit.getClass().getName() + ", OldSplit: " + oldInputSplit);
      setupOldRecordReader();
    }
    LOG.info("Initialized RecordReader from event");
  }

  @Private
  public static InputSplit getOldSplitDetailsFromEvent(MRSplitProto splitProto, Configuration conf)
      throws IOException {
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    return MRHelpers.createOldFormatSplitFromUserPayload(splitProto, serializationFactory);
  }
  
  @SuppressWarnings("unchecked")
  private InputSplit getOldSplitDetailsFromDisk(TaskSplitIndex splitMetaInfo)
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

  @Private
  public static org.apache.hadoop.mapreduce.InputSplit getNewSplitDetailsFromEvent(
      MRSplitProto splitProto, Configuration conf) throws IOException {
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    return MRHelpers.createNewFormatSplitFromUserPayload(
        splitProto, serializationFactory);
  }
  
  @SuppressWarnings("unchecked")
  private org.apache.hadoop.mapreduce.InputSplit getNewSplitDetailsFromDisk(
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

  protected TaskSplitMetaInfo[] readSplits(Configuration conf)
      throws IOException {
    TaskSplitMetaInfo[] allTaskSplitMetaInfo;
    allTaskSplitMetaInfo = SplitMetaInfoReaderTez.readSplitMetaInfo(conf,
        FileSystem.getLocal(conf));
    return allTaskSplitMetaInfo;
  }
  
  private class MRInputKVReader implements KeyValueReader {
    
    Object key;
    Object value;

    private final boolean localNewApi;
    
    MRInputKVReader() {
      localNewApi = useNewApi;
      if (!localNewApi) {
        key = oldRecordReader.createKey();
        value =oldRecordReader.createValue();
      }
    }
    
    // Setup the values iterator once, and set value on the same object each time
    // to prevent lots of objects being created.

    
    @SuppressWarnings("unchecked")
    @Override
    public boolean next() throws IOException {
      boolean hasNext = false;
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
      if (hasNext) {
        inputRecordCounter.increment(1);
      }
      
      return hasNext;
    }

    @Override
    public Object getCurrentKey() throws IOException {
      if (localNewApi) {
        try {
          return newRecordReader.getCurrentKey();
        } catch (InterruptedException e) {
          throw new IOException("Interrupted while fetching next key", e);
        }
      } else {
        return key;
      }
    }

    @Override
    public Object getCurrentValue() throws IOException {
      if (localNewApi) {
        try {
          return newRecordReader.getCurrentValue();
        } catch (InterruptedException e) {
          throw new IOException("Interrupted while fetching next value", e);
        }
      } else {
        return value;
      }
    }
  }

}
