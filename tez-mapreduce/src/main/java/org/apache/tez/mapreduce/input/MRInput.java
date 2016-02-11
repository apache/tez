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

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.security.Credentials;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.mapreduce.common.MRInputAMSplitGenerator;
import org.apache.tez.mapreduce.common.MRInputSplitDistributor;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.base.MRInputBase;
import org.apache.tez.mapreduce.lib.MRInputUtils;
import org.apache.tez.mapreduce.lib.MRReader;
import org.apache.tez.mapreduce.lib.MRReaderMapReduce;
import org.apache.tez.mapreduce.lib.MRReaderMapred;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

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
@Public
public class MRInput extends MRInputBase {

  @Private public static final String TEZ_MAPREDUCE_DAG_INDEX = "tez.mapreduce.dag.index";
  @Private public static final String TEZ_MAPREDUCE_DAG_NAME = "tez.mapreduce.dag.name";
  @Private public static final String TEZ_MAPREDUCE_VERTEX_INDEX = "tez.mapreduce.vertex.index";
  @Private public static final String TEZ_MAPREDUCE_VERTEX_NAME = "tez.mapreduce.vertex.name";
  @Private public static final String TEZ_MAPREDUCE_TASK_INDEX = "tez.mapreduce.task.index";
  @Private public static final String TEZ_MAPREDUCE_TASK_ATTEMPT_INDEX = "tez.mapreduce.task.attempt.index";
  @Private public static final String TEZ_MAPREDUCE_INPUT_INDEX = "tez.mapreduce.input.index";
  @Private public static final String TEZ_MAPREDUCE_INPUT_NAME = "tez.mapreduce.input.name";
  @Private public static final String TEZ_MAPREDUCE_APPLICATION_ID = "tez.mapreduce.application.id";
  @Private public static final String TEZ_MAPREDUCE_UNIQUE_IDENTIFIER = "tez.mapreduce.unique.identifier";
  @Private public static final String TEZ_MAPREDUCE_DAG_ATTEMPT_NUMBER = "tez.mapreduce.dag.attempt.number";



  /**
   * Helper class to configure {@link MRInput}
   *
   */
  public static class MRInputConfigBuilder {
    final Configuration conf;
    final Class<?> inputFormat;
    final boolean inputFormatProvided;
    boolean useNewApi;
    boolean groupSplitsInAM = true;
    boolean generateSplitsInAM = true;
    String inputClassName = MRInput.class.getName();
    boolean getCredentialsForSourceFilesystem = true;
    String inputPaths = null;
    InputInitializerDescriptor customInitializerDescriptor = null;

    MRInputConfigBuilder(Configuration conf, Class<?> inputFormatParam) {
      this.conf = conf;
      if (inputFormatParam != null) {
        inputFormatProvided = true;
        this.inputFormat = inputFormatParam;
        if (org.apache.hadoop.mapred.InputFormat.class.isAssignableFrom(inputFormatParam)) {
          useNewApi = false;
        } else if (org.apache.hadoop.mapreduce.InputFormat.class.isAssignableFrom(inputFormatParam)) {
          useNewApi = true;
        } else {
          throw new TezUncheckedException("inputFormat must be assignable from either " +
              "org.apache.hadoop.mapred.InputFormat or " +
              "org.apache.hadoop.mapreduce.InputFormat" +
              " Given: " + inputFormatParam.getName());
        }
      } else {
        inputFormatProvided = false;
        useNewApi = conf.getBoolean(MRJobConfig.NEW_API_MAPPER_CONFIG, true);
        try {
          if (useNewApi) {
            this.inputFormat = conf.getClassByName(conf.get(MRJobConfig.INPUT_FORMAT_CLASS_ATTR));
            Preconditions.checkState(org.apache.hadoop.mapreduce.InputFormat.class
                .isAssignableFrom(this.inputFormat));
          } else {
            this.inputFormat = conf.getClassByName(conf.get("mapred.input.format.class"));
            Preconditions.checkState(org.apache.hadoop.mapred.InputFormat.class
                .isAssignableFrom(this.inputFormat));
          }
        } catch (ClassNotFoundException e) {
          throw new TezUncheckedException(e);
        }
        initializeInputPath();
      }
    }
    
    MRInputConfigBuilder setInputClassName(String className) {
      this.inputClassName = className;
      return this;
    }

    private MRInputConfigBuilder setInputPaths(String inputPaths) {
      if (!(org.apache.hadoop.mapred.FileInputFormat.class.isAssignableFrom(inputFormat) || 
          FileInputFormat.class.isAssignableFrom(inputFormat))) {
        throw new TezUncheckedException("When setting inputPaths the inputFormat must be " + 
            "assignable from either org.apache.hadoop.mapred.FileInputFormat or " +
            "org.apache.hadoop.mapreduce.lib.input.FileInputFormat. " +
            "Otherwise use the non-path configBuilder." +
            " Given: " + inputFormat.getName());
      }
      conf.set(FileInputFormat.INPUT_DIR, inputPaths);
      this.inputPaths = inputPaths;
      return this;
    }

    private void initializeInputPath() {
      Preconditions.checkState(inputFormatProvided == false,
          "Should only be invoked when no inputFormat is provided");
      if (org.apache.hadoop.mapred.FileInputFormat.class.isAssignableFrom(inputFormat) ||
          FileInputFormat.class.isAssignableFrom(inputFormat)) {
        inputPaths = conf.get(FileInputFormat.INPUT_DIR);
      }
    }

    /**
     * Set whether splits should be grouped (default true)
     * @param value whether to group splits in the AM or not
     * @return {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder}
     */
    public MRInputConfigBuilder groupSplits(boolean value) {
      groupSplitsInAM = value;
      return this;
    }
    
    /**
     * Set whether splits should be generated in the Tez App Master (default true)
     * @param value whether to generate splits in the AM or not
     * @return {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder}
     */
    public MRInputConfigBuilder generateSplitsInAM(boolean value) {
      generateSplitsInAM = value;
      return this;
    }

    /**
     * Get the credentials for the inputPaths from their {@link FileSystem}s
     * Use the method to turn this off when not using a {@link FileSystem}
     * or when {@link Credentials} are not supported
     * @param value whether to get credentials or not. (true by default)
     * @return {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder}
     */
    public MRInputConfigBuilder getCredentialsForSourceFileSystem(boolean value) {
      getCredentialsForSourceFilesystem = value;
      return this;
    }

    /**
     * This method is intended to be used in case a custom {@link org.apache.tez.runtime.api.InputInitializer}
     * is being used along with MRInput. If a custom descriptor is used, the config builder will not be
     * able to setup location hints, parallelism, etc, and configuring the {@link
     * org.apache.tez.dag.api.Vertex} on which this Input is used is the responsibility of the user.
     *
     * Credential fetching can be controlled via the {@link #getCredentialsForSourceFilesystem} method.
     * Whether grouping is enabled or not can be controlled via {@link #groupSplitsInAM} method.
     *
     * @param customInitializerDescriptor the initializer descriptor
     * @return {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder}
     */
    public MRInputConfigBuilder setCustomInitializerDescriptor(
        InputInitializerDescriptor customInitializerDescriptor) {
      this.customInitializerDescriptor = customInitializerDescriptor;
      return this;
    }

    /**
     * Create the {@link DataSourceDescriptor}
     *
     * @return {@link DataSourceDescriptor}
     */
    public DataSourceDescriptor build() {
      if (org.apache.hadoop.mapred.FileInputFormat.class.isAssignableFrom(inputFormat) ||
          FileInputFormat.class.isAssignableFrom(inputFormat)) {
        if (inputPaths == null) {
          throw new TezUncheckedException(
              "InputPaths must be specified for InputFormats based on " +
                  FileInputFormat.class.getName() + " or " +
                  org.apache.hadoop.mapred.FileInputFormat.class.getName());
        }
      }
      try {
        if (this.customInitializerDescriptor != null) {
          return createCustomDataSource();
        } else {
          if (generateSplitsInAM) {
            return createGeneratorDataSource();
          } else {
            return createDistributorDataSource();
          }
        }
      } catch (Exception e) {
        throw new TezUncheckedException(e);
      }
    }
    
    private DataSourceDescriptor createDistributorDataSource() throws IOException {
      InputSplitInfo inputSplitInfo;
      setupBasicConf(conf);
      try {
        inputSplitInfo = MRInputHelpers.generateInputSplitsToMem(conf, false, 0);
      } catch (Exception e) {
        throw new TezUncheckedException(e);
      }
      MRHelpers.translateMRConfToTez(conf);

      UserPayload payload = MRInputHelpersInternal.createMRInputPayload(conf,
          inputSplitInfo.getSplitsProto());
      Credentials credentials = null;
      if (getCredentialsForSourceFilesystem && inputSplitInfo.getCredentials() != null) {
        credentials = inputSplitInfo.getCredentials();
      }
      DataSourceDescriptor ds = DataSourceDescriptor.create(
          InputDescriptor.create(inputClassName).setUserPayload(payload),
          InputInitializerDescriptor.create(MRInputSplitDistributor.class.getName()),
          inputSplitInfo.getNumTasks(), credentials,
          VertexLocationHint.create(inputSplitInfo.getTaskLocationHints()), null);
      if (conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT,
          TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT_DEFAULT)) {
        ds.getInputDescriptor().setHistoryText(TezUtils.convertToHistoryText(conf));
      }

      return ds;
    }

    private DataSourceDescriptor createCustomDataSource() throws IOException {
      setupBasicConf(conf);

      MRHelpers.translateMRConfToTez(conf);

      Collection<URI> uris = maybeGetURIsForCredentials();

      UserPayload payload = null;
      if (groupSplitsInAM) {
        payload = MRInputHelpersInternal.createMRInputPayloadWithGrouping(conf);
      } else {
        payload = MRInputHelpersInternal.createMRInputPayload(conf, null);
      }

      DataSourceDescriptor ds = DataSourceDescriptor
          .create(InputDescriptor.create(inputClassName).setUserPayload(payload),
              customInitializerDescriptor, null);

      if (conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT,
          TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT_DEFAULT)) {
        ds.getInputDescriptor().setHistoryText(TezUtils.convertToHistoryText(conf));
      }

      if (uris != null) {
        ds.addURIsForCredentials(uris);
      }
      return ds;
    }

    private DataSourceDescriptor createGeneratorDataSource() throws IOException {
      setupBasicConf(conf);
      MRHelpers.translateMRConfToTez(conf);
      
      Collection<URI> uris = maybeGetURIsForCredentials();

      UserPayload payload = null;
      if (groupSplitsInAM) {
        payload = MRInputHelpersInternal.createMRInputPayloadWithGrouping(conf);
      } else {
        payload = MRInputHelpersInternal.createMRInputPayload(conf, null);
      }

      DataSourceDescriptor ds = DataSourceDescriptor.create(
          InputDescriptor.create(inputClassName).setUserPayload(payload),
          InputInitializerDescriptor.create(MRInputAMSplitGenerator.class.getName()), null);

      if (conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT,
          TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT_DEFAULT)) {
        ds.getInputDescriptor().setHistoryText(TezUtils.convertToHistoryText(conf));
      }

      if (uris != null) {
        ds.addURIsForCredentials(uris);
      }
      return ds;
    }

    private void setupBasicConf(Configuration inputConf) {
      if (inputFormatProvided) {
        inputConf.setBoolean(MRJobConfig.NEW_API_MAPPER_CONFIG, useNewApi);
        if (useNewApi) {
          inputConf.set(MRJobConfig.INPUT_FORMAT_CLASS_ATTR, inputFormat.getName());
        } else {
          inputConf.set("mapred.input.format.class", inputFormat.getName());
        }
      }
    }

    private Collection<URI> maybeGetURIsForCredentials() {
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
          return uris;
        } catch (IOException e) {
          throw new TezUncheckedException(e);
        }
      }
      return null;
    }

  }

  /**
   * Create an {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder} </p>
   * The preferred usage model is to provide all of the parameters, and use methods to configure
   * the Input.
   * <p/>
   * For legacy applications, which may already have a fully configured {@link Configuration}
   * instance, the inputFormat can be specified as null
   *
   * @param conf        Configuration for the {@link MRInput}. This configuration instance will be
   *                    modified in place
   * @param inputFormat InputFormat derived class. This can be null. If the InputFormat specified
   *                    is
   *                    null, the provided configuration should be complete.
   * @return {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder}
   */
  public static MRInputConfigBuilder createConfigBuilder(Configuration conf,
                                                         @Nullable Class<?> inputFormat) {
    return new MRInputConfigBuilder(conf, inputFormat);
  }

  /**
   * Create an {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder} 
   * for {@link org.apache.hadoop.mapreduce.lib.input.FileInputFormat}
   * or {@link org.apache.hadoop.mapred.FileInputFormat} format based InputFormats.
   * <p/>
   * The preferred usage model is to provide all of the parameters, and use methods to configure
   * the Input.
   * <p/>
   * For legacy applications, which may already have a fully configured {@link Configuration}
   * instance, the inputFormat and inputPath can be specified as null
   *
   * @param conf        Configuration for the {@link MRInput}. This configuration instance will be
   *                    modified in place
   * @param inputFormat InputFormat derived class. This can be null. If the InputFormat specified
   *                    is
   *                    null, the provided configuration should be complete.
   * @param inputPaths  Comma separated input paths
   * @return {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder}
   */
  public static MRInputConfigBuilder createConfigBuilder(Configuration conf,
                                                         @Nullable Class<?> inputFormat,
                                                         @Nullable String inputPaths) {
    MRInputConfigBuilder configurer = new MRInputConfigBuilder(conf, inputFormat);
    if (inputPaths != null) {
      return configurer.setInputPaths(inputPaths);
    }
    return configurer;
  }

  private static final Logger LOG = LoggerFactory.getLogger(MRInput.class);
  
  private final ReentrantLock rrLock = new ReentrantLock();
  private final Condition rrInited = rrLock.newCondition();
  
  private volatile boolean eventReceived = false;

  private boolean readerCreated = false;

  protected MRReader mrReader;

  protected TaskSplitIndex splitMetaInfo = new TaskSplitIndex();

  // Potential counters - #splits, #totalSize, #actualyBytesRead
  
  @Private
  volatile boolean splitInfoViaEvents;

  public MRInput(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Override
  public List<Event> initialize() throws IOException {
    super.initialize();
    getContext().inputIsReady();
    this.splitInfoViaEvents = jobConf.getBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS,
        MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS_DEFAULT);
    LOG.info(getContext().getSourceVertexName() + " using newmapreduce API=" + useNewApi +
        ", split via event=" + splitInfoViaEvents + ", numPhysicalInputs=" +
        getNumPhysicalInputs());
    initializeInternal();
    return null;
  }

  @Override
  public void start() {
    Preconditions.checkState(getNumPhysicalInputs() == 0 || getNumPhysicalInputs() == 1,
        "Expecting 0 or 1 physical input for MRInput");
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
                  .getTaskIndex(), getContext().getTaskAttemptNumber(), getContext());
        } else {
          mrReader = new MRReaderMapred(jobConf, getContext().getCounters(), inputRecordCounter, 
              getContext());
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
              getContext().getTaskIndex(), getContext().getTaskAttemptNumber(), getContext());
        } else {
          org.apache.hadoop.mapred.InputSplit oldInputSplit = MRInputUtils
              .getOldSplitDetailsFromDisk(splitMetaInfo, jobConf, getContext().getCounters()
                  .findCounter(TaskCounter.SPLIT_RAW_BYTES));
          mrReader =
              new MRReaderMapred(jobConf, oldInputSplit, getContext().getCounters(),
                  inputRecordCounter, getContext());
        }
      }
    } finally {
      rrLock.unlock();
    }
    LOG.info("Initialized MRInput: " + getContext().getSourceVertexName());
  }

  /**
   * Returns a {@link KeyValueReader} that can be used to read 
   * Map Reduce compatible key value data. An exception will be thrown if next()
   * is invoked after false, either from the framework or from the underlying InputFormat
   */
  @Override
  public KeyValueReader getReader() throws IOException {
    Preconditions
        .checkState(readerCreated == false,
            "Only a single instance of record reader can be created for this input.");
    readerCreated = true;
    if (getNumPhysicalInputs() == 0) {
      return new KeyValueReader() {
        @Override
        public boolean next() throws IOException {
          getContext().notifyProgress();
          return false;
        }

        @Override
        public Object getCurrentKey() throws IOException {
          return null;
        }

        @Override
        public Object getCurrentValue() throws IOException {
          return null;
        }
      };
    }
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
    if (getNumPhysicalInputs() == 0) {
      throw new IllegalStateException(
          "Unexpected event. MRInput has been setup to receive 0 events");
    }
    if (eventReceived || inputEvents.size() != 1) {
      throw new IllegalStateException(
          "MRInput expects only a single input. Received: current eventListSize: "
              + inputEvents.size() + "Received previous input: "
              + eventReceived);
    }
    Event event = inputEvents.iterator().next();
    Preconditions.checkArgument(event instanceof InputDataInformationEvent,
        getClass().getSimpleName()
            + " can only handle a single event of type: "
            + InputDataInformationEvent.class.getSimpleName());

    processSplitEvent((InputDataInformationEvent) event);
  }

  @Override
  public List<Event> close() throws IOException {
    mrReader.close();
    long inputRecords = getContext().getCounters()
        .findCounter(TaskCounter.INPUT_RECORDS_PROCESSED).getValue();
    getContext().getStatisticsReporter().reportItemsProcessed(inputRecords);

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

  void processSplitEvent(InputDataInformationEvent event)
      throws IOException {
    rrLock.lock();
    try {
      initFromEventInternal(event);
      if (LOG.isDebugEnabled()) {
        LOG.debug(getContext().getSourceVertexName() + " notifying on RecordReader initialized");
      }
      rrInited.signal();
    } finally {
      rrLock.unlock();
    }
  }
  
  void checkAndAwaitRecordReaderInitialization() throws IOException {
    assert rrLock.getHoldCount() == 1;
    rrLock.lock();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug(getContext().getSourceVertexName() + " awaiting RecordReader initialization");
      }
      rrInited.await();
    } catch (Exception e) {
      throw new IOException(
          "Interrupted waiting for RecordReader initiailization");
    } finally {
      rrLock.unlock();
    }
  }

  @Private
  void initFromEvent(InputDataInformationEvent initEvent)
      throws IOException {
    rrLock.lock();
    try {
      initFromEventInternal(initEvent);
    } finally {
      rrLock.unlock();
    }
  }
  
  private void initFromEventInternal(InputDataInformationEvent initEvent) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(getContext().getSourceVertexName() + " initializing RecordReader from event");
    }
    Preconditions.checkState(initEvent != null, "InitEvent must be specified");
    MRSplitProto splitProto = MRSplitProto.parseFrom(ByteString.copyFrom(initEvent.getUserPayload()));
    Object split = null;
    if (useNewApi) {
      split = MRInputUtils.getNewSplitDetailsFromEvent(splitProto, jobConf);
      if (LOG.isDebugEnabled()) {
        LOG.debug(getContext().getSourceVertexName() + " split Details -> SplitClass: " +
            split.getClass().getName() + ", NewSplit: "
            + split);
      }

    } else {
      split = MRInputUtils.getOldSplitDetailsFromEvent(splitProto, jobConf);
      if (LOG.isDebugEnabled()) {
        LOG.debug(getContext().getSourceVertexName() + " split Details -> SplitClass: " +
            split.getClass().getName() + ", OldSplit: "
            + split);
      }
    }
    mrReader.setSplit(split);
    LOG.info(getContext().getSourceVertexName() + " initialized RecordReader from event");
  }

  private static class MRInputHelpersInternal extends MRInputHelpers {

    protected static UserPayload createMRInputPayloadWithGrouping(Configuration conf) throws
        IOException {
      return MRInputHelpers.createMRInputPayloadWithGrouping(conf);
    }

    protected static UserPayload createMRInputPayload(Configuration conf,
                                                 MRRuntimeProtos.MRSplitsProto mrSplitsProto) throws
        IOException {
      return MRInputHelpers.createMRInputPayload(conf, mrSplitsProto);
    }
  }

}
