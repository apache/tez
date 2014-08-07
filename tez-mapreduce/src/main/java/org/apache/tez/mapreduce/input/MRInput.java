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
import java.net.URI;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.common.MRInputSplitDistributor;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.base.MRInputBase;
import org.apache.tez.mapreduce.lib.MRInputUtils;
import org.apache.tez.mapreduce.lib.MRReader;
import org.apache.tez.mapreduce.lib.MRReaderMapReduce;
import org.apache.tez.mapreduce.lib.MRReaderMapred;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.library.api.KeyValueReader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * {@link MRInput} is an {@link Input} which provides key/values pairs
 * for the consumer.
 *
 * It is compatible with all standard Apache Hadoop MapReduce 
 * {@link InputFormat} implementations.
 * 
 * This class is not meant to be extended by external projects.
 */

public class MRInput extends MRInputBase {

  /**
   * Helper class to configure {@link MRInput}
   *
   */
  public static class MRInputConfigurer {
    final Configuration conf;
    final Class<?> inputFormat;
    boolean useNewApi;
    boolean groupSplitsInAM = true;
    boolean generateSplitsInAM = true;
    String inputClassName = MRInput.class.getName();
    boolean getCredentialsForSourceFilesystem = true;
    String inputPaths = null;
    
    private MRInputConfigurer(Configuration conf, Class<?> inputFormat) {
      this.conf = conf;
      this.inputFormat = inputFormat;
      if (org.apache.hadoop.mapred.InputFormat.class.isAssignableFrom(inputFormat)) {
        useNewApi = false;
      } else if(org.apache.hadoop.mapreduce.InputFormat.class.isAssignableFrom(inputFormat)) {
        useNewApi = true;
      } else {
        throw new TezUncheckedException("inputFormat must be assignable from either " +
            "org.apache.hadoop.mapred.InputFormat or " +
            "org.apache.hadoop.mapreduce.InputFormat" +
            " Given: " + inputFormat.getName());
      }
    }
    
    MRInputConfigurer setInputClassName(String className) {
      this.inputClassName = className;
      return this;
    }

    private MRInputConfigurer setInputPaths(String inputPaths) {
      if (!(org.apache.hadoop.mapred.FileInputFormat.class.isAssignableFrom(inputFormat) || 
          FileInputFormat.class.isAssignableFrom(inputFormat))) {
        throw new TezUncheckedException("When setting inputPaths the inputFormat must be " + 
            "assignable from either org.apache.hadoop.mapred.FileInputFormat or " +
            "org.apache.hadoop.mapreduce.lib.input.FileInputFormat. " +
            "Otherwise use the non-path configurer." +
            " Given: " + inputFormat.getName());
      }
      conf.set(FileInputFormat.INPUT_DIR, inputPaths);
      this.inputPaths = inputPaths;
      return this;
    }
    
    /**
     * Set whether splits should be grouped in the Tez App Master (default true)
     * @param value whether to group splits in the AM or not
     * @return {@link MRInputConfigurer}
     */
    public MRInputConfigurer groupSplitsInAM(boolean value) {
      groupSplitsInAM = value;
      return this;
    }
    
    /**
     * Set whether splits should be generated in the Tez App Master (default true)
     * @param value whether to generate splits in the AM or not
     * @return {@link MRInputConfigurer}
     */
    public MRInputConfigurer generateSplitsInAM(boolean value) {
      generateSplitsInAM = value;
      return this;
    }
    
    /**
     * Get the credentials for the inputPaths from their {@link FileSystem}s
     * Use the method to turn this off when not using a {@link FileSystem}
     * or when {@link Credentials} are not supported
     * @param value whether to get credentials or not. (true by default)
     * @return {@link MRInputConfigurer}
     */
    public MRInputConfigurer getCredentialsForSourceFileSystem(boolean value) {
      getCredentialsForSourceFilesystem = value;
      return this;
    }
        
    /**
     * Create the {@link DataSourceDescriptor}
     * @return {@link DataSourceDescriptor}
     */
    public DataSourceDescriptor create() {
      try {
        if (generateSplitsInAM) {
          return createGeneratorDataSource();
        } else {
          return createDistributorDataSource();
        }
      } catch (Exception e) {
        throw new TezUncheckedException(e);
      } 
    }
    
    private DataSourceDescriptor createDistributorDataSource() throws IOException {
      Configuration inputConf = new JobConf(conf);
      InputSplitInfo inputSplitInfo;
      try {
        inputSplitInfo = MRHelpers.generateInputSplitsToMem(inputConf, false, 0);
      } catch (Exception e) {
        throw new TezUncheckedException(e);
      }
      inputConf.setBoolean("mapred.mapper.new-api", useNewApi);
      if (useNewApi) {
        inputConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, inputFormat.getName());
      } else {
        inputConf.set("mapred.input.format.class", inputFormat.getName());
      }
      MRHelpers.translateVertexConfToTez(inputConf);
      MRHelpers.doJobClientMagic(inputConf);
      byte[] payload = MRHelpers.createMRInputPayload(inputConf, inputSplitInfo.getSplitsProto());
      Credentials credentials = null;
      if (getCredentialsForSourceFilesystem && inputSplitInfo.getCredentials() != null) {
        credentials = inputSplitInfo.getCredentials();
      }
      return new DataSourceDescriptor(
          new InputDescriptor(inputClassName).setUserPayload(payload),
          new InputInitializerDescriptor(MRInputSplitDistributor.class.getName()),
          inputSplitInfo.getNumTasks(), credentials, 
          new VertexLocationHint(inputSplitInfo.getTaskLocationHints()));
    }
    
    private DataSourceDescriptor createGeneratorDataSource() throws IOException {
      Configuration inputConf = new JobConf(conf);
      inputConf.setBoolean("mapred.mapper.new-api", useNewApi);
      if (useNewApi) {
        inputConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, inputFormat.getName());
      } else {
        inputConf.set("mapred.input.format.class", inputFormat.getName());
      }
      MRHelpers.translateVertexConfToTez(inputConf);
      MRHelpers.doJobClientMagic(inputConf);
      
      Credentials credentials = null;
      if (getCredentialsForSourceFilesystem && inputPaths != null) {
        try {
          List<URI> uris = Lists.newLinkedList();
          for (String inputPath : inputPaths.split(",")) {
            Path path = new Path(inputPath);
            FileSystem fs;
            fs = path.getFileSystem(conf);
            Path qPath = fs.makeQualified(path);
            uris.add(qPath.toUri());
          }
          credentials = new Credentials();
          TezClientUtils.addFileSystemCredentialsFromURIs(uris, credentials, conf);
        } catch (IOException e) {
          throw new TezUncheckedException(e);
        }
      }

      byte[] payload = null;
      if (groupSplitsInAM) {
        payload = MRHelpers.createMRInputPayloadWithGrouping(inputConf);
      } else {
        payload = MRHelpers.createMRInputPayload(inputConf, null);
      }
      return new DataSourceDescriptor(
          new InputDescriptor(inputClassName).setUserPayload(payload),
          new InputInitializerDescriptor(MRInputAMSplitGenerator.class.getName()), credentials);
    }
  }
  
  /**
   * Create an {@link MRInputConfigurer}
   * @param conf Configuration for the {@link MRInput}
   * @param inputFormat InputFormat derived class
   * @return {@link MRInputConfigurer}
   */
  public static MRInputConfigurer createConfigurer(Configuration conf, Class<?> inputFormat) {
    return new MRInputConfigurer(conf, inputFormat);
  }

  /**
   * Create an {@link MRInputConfigurer} for a FileInputFormat
   * @param conf Configuration for the {@link MRInput}
   * @param inputFormat FileInputFormat derived class
   * @param inputPaths Comma separated input paths
   * @return {@link MRInputConfigurer}
   */
  public static MRInputConfigurer createConfigurer(Configuration conf, Class<?> inputFormat,
      String inputPaths) {
    return new MRInputConfigurer(conf, inputFormat).setInputPaths(inputPaths);
  }
  
  private static final Log LOG = LogFactory.getLog(MRInput.class);
  
  private final ReentrantLock rrLock = new ReentrantLock();
  private final Condition rrInited = rrLock.newCondition();
  
  private volatile boolean eventReceived = false;

  private boolean readerCreated = false;

  protected MRReader mrReader;

  protected TaskSplitIndex splitMetaInfo = new TaskSplitIndex();

  // Potential counters - #splits, #totalSize, #actualyBytesRead
  
  @Private
  volatile boolean splitInfoViaEvents;

  public MRInput(TezInputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Override
  public List<Event> initialize() throws IOException {
    super.initialize();
    getContext().inputIsReady();
    this.splitInfoViaEvents = jobConf.getBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS,
        MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS_DEFAULT);
    LOG.info("Using New mapreduce API: " + useNewApi
        + ", split information via event: " + splitInfoViaEvents);
    initializeInternal();
    return null;
  }

  @Override
  public void start() {
    Preconditions.checkState(getNumPhysicalInputs() == 1, "Expecting only 1 physical input for MRInput");
  }

  @Private
  void initializeInternal() throws IOException {
    // Primarily for visibility
    rrLock.lock();
    try {
      
      if (splitInfoViaEvents) {
        if (useNewApi) {
          mrReader = new MRReaderMapReduce(jobConf, getContext().getCounters(), inputRecordCounter,
              getContext().getApplicationId().getClusterTimestamp(), getContext()
                  .getTaskVertexIndex(), getContext().getApplicationId().getId(), getContext()
                  .getTaskIndex(), getContext().getTaskAttemptNumber());
        } else {
          mrReader = new MRReaderMapred(jobConf, getContext().getCounters(), inputRecordCounter);
        }
      } else {
        TaskSplitMetaInfo[] allMetaInfo = MRInputUtils.readSplits(jobConf);
        TaskSplitMetaInfo thisTaskMetaInfo = allMetaInfo[getContext().getTaskIndex()];
        TaskSplitIndex splitMetaInfo = new TaskSplitIndex(thisTaskMetaInfo.getSplitLocation(),
            thisTaskMetaInfo.getStartOffset());
        if (useNewApi) {
          org.apache.hadoop.mapreduce.InputSplit newInputSplit = MRInputUtils
              .getNewSplitDetailsFromDisk(splitMetaInfo, jobConf, getContext().getCounters()
                  .findCounter(TaskCounter.SPLIT_RAW_BYTES));
          mrReader = new MRReaderMapReduce(jobConf, newInputSplit, getContext().getCounters(),
              inputRecordCounter, getContext().getApplicationId().getClusterTimestamp(),
              getContext().getTaskVertexIndex(), getContext().getApplicationId().getId(),
              getContext().getTaskIndex(), getContext().getTaskAttemptNumber());
        } else {
          org.apache.hadoop.mapred.InputSplit oldInputSplit = MRInputUtils
              .getOldSplitDetailsFromDisk(splitMetaInfo, jobConf, getContext().getCounters()
                  .findCounter(TaskCounter.SPLIT_RAW_BYTES));
          mrReader =
              new MRReaderMapred(jobConf, oldInputSplit, getContext().getCounters(),
                  inputRecordCounter);
        }
      }
    } finally {
      rrLock.unlock();
    }
    LOG.info("Initialzed MRInput: " + getContext().getSourceVertexName());
  }

  @Override
  public KeyValueReader getReader() throws IOException {
    Preconditions
        .checkState(readerCreated == false,
            "Only a single instance of record reader can be created for this input.");
    readerCreated = true;
    rrLock.lock();
    try {
      if (!mrReader.isSetup())
        checkAndAwaitRecordReaderInitialization();
    } finally {
      rrLock.unlock();
    }

    return mrReader;
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
  public List<Event> close() throws IOException {
    mrReader.close();
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
    if (!useNewApi) {
      return ((MRReaderMapred) mrReader).getConfigUpdates();
    } else {
      return null;
    }
  }

  public float getProgress() throws IOException, InterruptedException {
    return mrReader.getProgress();
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
    assert rrLock.getHoldCount() == 1;
    rrLock.lock();
    try {
      LOG.info("Awaiting RecordReader initialization");
      rrInited.await();
    } catch (Exception e) {
      throw new IOException(
          "Interrupted waiting for RecordReader initiailization");
    } finally {
      rrLock.unlock();
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
  
  private void initFromEventInternal(RootInputDataInformationEvent initEvent) throws IOException {
    LOG.info("Initializing RecordReader from event");
    Preconditions.checkState(initEvent != null, "InitEvent must be specified");
    MRSplitProto splitProto = MRSplitProto.parseFrom(initEvent.getUserPayload());
    Object split = null;
    if (useNewApi) {
      split = MRInputUtils.getNewSplitDetailsFromEvent(splitProto, jobConf);
      LOG.info("Split Details -> SplitClass: " + split.getClass().getName() + ", NewSplit: "
          + split);

    } else {
      split = MRInputUtils.getOldSplitDetailsFromEvent(splitProto, jobConf);
      LOG.info("Split Details -> SplitClass: " + split.getClass().getName() + ", OldSplit: "
          + split);
    }
    mrReader.setSplit(split);
    LOG.info("Initialized RecordReader from event");
  }
}
