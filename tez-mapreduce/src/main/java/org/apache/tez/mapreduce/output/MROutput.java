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

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.tez.common.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.OutputCommitterDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.mapreduce.committer.MROutputCommitter;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.mapred.MRReporter;
import org.apache.tez.mapreduce.hadoop.mapreduce.TaskAttemptContextImpl;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Output;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

/**
 * {@link MROutput} is an {@link Output} which allows key/values pairs
 * to be written by a processor.
 *
 * It is compatible with all standard Apache Hadoop MapReduce
 * OutputFormat implementations.
 *
 * This class is not meant to be extended by external projects.
 */
@Public
public class MROutput extends AbstractLogicalOutput {

  /**
   * Helper class to configure {@link MROutput}
   *
   */
  public static class MROutputConfigBuilder {
    final Configuration conf;
    final Class<?> outputFormat;
    final boolean outputFormatProvided;
    boolean useNewApi;
    boolean getCredentialsForSinkFilesystem = true;
    String outputClassName = MROutput.class.getName();
    String outputPath;
    boolean doCommit = true;

    private MROutputConfigBuilder(Configuration conf,
        Class<?> outputFormatParam, boolean useLazyOutputFormat) {
      this.conf = conf;
      if (outputFormatParam != null) {
        outputFormatProvided = true;
        if (org.apache.hadoop.mapred.OutputFormat.class.isAssignableFrom(
            outputFormatParam)) {
          useNewApi = false;
          if (!useLazyOutputFormat) {
            this.outputFormat = outputFormatParam;
          } else {
            conf.setClass(MRJobConfig.LAZY_OUTPUTFORMAT_OUTPUTFORMAT,
                outputFormatParam,
                org.apache.hadoop.mapred.OutputFormat.class);
            this.outputFormat =
                org.apache.hadoop.mapred.lib.LazyOutputFormat.class;
          }
        } else if (OutputFormat.class.isAssignableFrom(outputFormatParam)) {
          useNewApi = true;
          if (!useLazyOutputFormat) {
            this.outputFormat = outputFormatParam;
          } else {
            conf.setClass(MRJobConfig.LAZY_OUTPUTFORMAT_OUTPUTFORMAT,
                outputFormatParam, OutputFormat.class);
            this.outputFormat = LazyOutputFormat.class;
          }
        } else {
          throw new TezUncheckedException(
              "outputFormat must be assignable from either " +
              "org.apache.hadoop.mapred.OutputFormat or " +
              "org.apache.hadoop.mapreduce.OutputFormat" +
              " Given: " + outputFormatParam.getName());
        }
      } else {
        outputFormatProvided = false;
        if (conf.get(MRJobConfig.NEW_API_REDUCER_CONFIG) == null) {
          useNewApi = conf.getBoolean(MRJobConfig.NEW_API_MAPPER_CONFIG, true);
        } else {
          useNewApi = conf.getBoolean(MRJobConfig.NEW_API_REDUCER_CONFIG, true);
        }
        try {
          if (useNewApi) {
            String outputClass = conf.get(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR);
            if (StringUtils.isEmpty(outputClass)) {
              throw new TezUncheckedException("no outputFormat setting on Configuration, useNewAPI:" + useNewApi);
            }
            this.outputFormat = conf.getClassByName(outputClass);
            Preconditions.checkState(org.apache.hadoop.mapreduce.OutputFormat.class
                .isAssignableFrom(this.outputFormat), "outputFormat must be assignable from "
                    + "org.apache.hadoop.mapreduce.OutputFormat");
          } else {
            String outputClass = conf.get("mapred.output.format.class");
            if (StringUtils.isEmpty(outputClass)) {
              throw new TezUncheckedException("no outputFormat setting on Configuration, useNewAPI:" + useNewApi);
            }
            this.outputFormat = conf.getClassByName(outputClass);
            Preconditions.checkState(org.apache.hadoop.mapred.OutputFormat.class
                .isAssignableFrom(this.outputFormat), "outputFormat must be assignable from "
                    + "org.apache.hadoop.mapred.OutputFormat");
          }
        } catch (ClassNotFoundException e) {
          throw new TezUncheckedException(e);
        }
        initializeOutputPath();
      }
    }

    private MROutputConfigBuilder setOutputPath(String outputPath) {
      boolean passNewLazyOutputFormatCheck =
          (LazyOutputFormat.class.isAssignableFrom(outputFormat)) &&
          org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.class.
              isAssignableFrom(conf.getClass(
                  MRJobConfig.LAZY_OUTPUTFORMAT_OUTPUTFORMAT, null));
      boolean passOldLazyOutputFormatCheck =
          (org.apache.hadoop.mapred.lib.LazyOutputFormat.class.
              isAssignableFrom(outputFormat)) &&
          FileOutputFormat.class.isAssignableFrom(conf.getClass(
              MRJobConfig.LAZY_OUTPUTFORMAT_OUTPUTFORMAT, null));

      if (!(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.class.
          isAssignableFrom(outputFormat) ||
          FileOutputFormat.class.isAssignableFrom(outputFormat) ||
          passNewLazyOutputFormatCheck || passOldLazyOutputFormatCheck)) {
        throw new TezUncheckedException("When setting outputPath the outputFormat must " +
            "be assignable from either org.apache.hadoop.mapred.FileOutputFormat or " +
            "org.apache.hadoop.mapreduce.lib.output.FileOutputFormat. " +
            "Otherwise use the non-path config builder." +
            " Given: " + outputFormat.getName());
      }
      conf.set(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR, outputPath);
      this.outputPath = outputPath;
      return this;
    }

    private void initializeOutputPath() {
      Preconditions.checkState(outputFormatProvided == false,
          "Should only be invoked when no outputFormat is provided");
      if (org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.class.isAssignableFrom(outputFormat) ||
          FileOutputFormat.class.isAssignableFrom(outputFormat)) {
        outputPath = conf.get(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR);
      }
    }
    
    /**
     * Create the {@link DataSinkDescriptor}
     * @return {@link DataSinkDescriptor}
     */
    public DataSinkDescriptor build() {
      if (org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.class
          .isAssignableFrom(outputFormat) ||
          FileOutputFormat.class.isAssignableFrom(outputFormat)) {
        if (outputPath == null) {
          throw new TezUncheckedException(
              "OutputPaths must be specified for OutputFormats based on " +
                  org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.class.getName() + " or " +
                  FileOutputFormat.class.getName());
        }
      }
      Collection<URI> uris = null;
      if (getCredentialsForSinkFilesystem && outputPath != null) {
        try {
          Path path = new Path(outputPath);
          FileSystem fs;
          fs = path.getFileSystem(conf);
          Path qPath = fs.makeQualified(path);
          uris = Collections.singletonList(qPath.toUri());
        } catch (IOException e) {
          throw new TezUncheckedException(e);
        }
      }

      DataSinkDescriptor ds = DataSinkDescriptor.create(
          OutputDescriptor.create(outputClassName).setUserPayload(createUserPayload()),
          (doCommit ? OutputCommitterDescriptor.create(
              MROutputCommitter.class.getName()) : null), null);
      if (conf.getBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT,
          TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT_DEFAULT)) {
        ds.getOutputDescriptor().setHistoryText(TezUtils.convertToHistoryText(conf));
      }

      if (uris != null) {
        ds.addURIsForCredentials(uris);
      }
      return ds;
    }
    
    /**
     * Get the credentials for the output from its {@link FileSystem}s
     * Use the method to turn this off when not using a {@link FileSystem}
     * or when {@link Credentials} are not supported
     * @param value whether to get credentials or not. (true by default)
     * @return {@link org.apache.tez.mapreduce.output.MROutput.MROutputConfigBuilder}
     */
    public MROutputConfigBuilder getCredentialsForSinkFileSystem(boolean value) {
      getCredentialsForSinkFilesystem = value;
      return this;
    }
    
    /**
     * Disable commit operations for the output (default: true)
     * If the value is set to false then no {@link org.apache.tez.runtime.api.OutputCommitter} will
     * be specified for the output
     */
    public MROutputConfigBuilder setDoCommit(boolean value) {
      doCommit = value;
      return this;
    }

    MROutputConfigBuilder setOutputClassName(String outputClassName) {
      this.outputClassName = outputClassName;
      return this;
    }

    /**
     * Creates the user payload to be set on the OutputDescriptor for MROutput
     */
    private UserPayload createUserPayload() {
      // set which api is being used always
      conf.setBoolean(MRJobConfig.NEW_API_REDUCER_CONFIG, useNewApi);
      conf.setBoolean(MRJobConfig.NEW_API_MAPPER_CONFIG, useNewApi);
      if (outputFormatProvided) {
        if (useNewApi) {
          conf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormat.getName());
        } else {
          conf.set("mapred.output.format.class", outputFormat.getName());
        }
      }
      MRHelpers.translateMRConfToTez(conf);
      try {
        return TezUtils.createUserPayloadFromConf(conf);
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }
  }

  /**
   * Create an {@link org.apache.tez.mapreduce.output.MROutput.MROutputConfigBuilder} </p>
   * <p/>
   * The preferred usage model is to provide all of the parameters, and use methods to configure
   * the Output.
   * <p/>
   * For legacy applications, which may already have a fully configured {@link Configuration}
   * instance, the outputFormat can be specified as null
   *
   * @param conf         Configuration for the {@link MROutput}. This configuration instance will be
   *                     modified in place
   * @param outputFormat OutputFormat derived class. If the OutputFormat specified is
   *                     null, the provided configuration should be complete.
   * @return {@link org.apache.tez.mapreduce.output.MROutput.MROutputConfigBuilder}
   */
  public static MROutputConfigBuilder createConfigBuilder(Configuration conf,
                                                          @Nullable Class<?> outputFormat) {
    return createConfigBuilder(conf, outputFormat, false);
  }

  public static MROutputConfigBuilder createConfigBuilder(Configuration conf,
      @Nullable Class<?> outputFormat, boolean useLazyOutputFormat) {
    return new MROutputConfigBuilder(conf, outputFormat, useLazyOutputFormat);
  }

  /**
   * Create an {@link org.apache.tez.mapreduce.output.MROutput.MROutputConfigBuilder} for a {@link org.apache.hadoop.mapreduce.lib.output.FileOutputFormat}
   * or {@link org.apache.hadoop.mapred.FileOutputFormat} based OutputFormats.
   * <p/>
   * The preferred usage model is to provide all of the parameters, and use methods to configure the
   * Output.
   * <p/>
   * For legacy applications, which may already have a fully configured {@link Configuration}
   * instance, the outputFormat and outputPath can be specified as null
   *
   * @param conf         Configuration for the {@link MROutput}. This configuration instance will be
   *                     modified in place
   * @param outputFormat FileInputFormat derived class. If the InputFormat specified is
   *                     null, the provided configuration should be complete.
   * @param outputPath   Output path. This can be null if already setup in the configuration
   * @return {@link org.apache.tez.mapreduce.output.MROutput.MROutputConfigBuilder}
   */
  public static MROutputConfigBuilder createConfigBuilder(Configuration conf,
      @Nullable Class<?> outputFormat, @Nullable String outputPath) {
    return createConfigBuilder(conf, outputFormat, outputPath, false);
  }

  public static MROutputConfigBuilder createConfigBuilder(Configuration conf,
      @Nullable Class<?> outputFormat, @Nullable String outputPath,
      boolean useLazyOutputFormat) {
    MROutputConfigBuilder configurer = createConfigBuilder(conf, outputFormat, useLazyOutputFormat);
    if (outputPath != null) {
      configurer.setOutputPath(outputPath);
    }
    return configurer;
  }

  private static final Logger LOG = LoggerFactory.getLogger(MROutput.class);

  private final NumberFormat taskNumberFormat = NumberFormat.getInstance();
  private final NumberFormat nonTaskNumberFormat = NumberFormat.getInstance();
  
  protected JobConf jobConf;
  boolean useNewApi;
  protected AtomicBoolean flushed = new AtomicBoolean(false);

  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapreduce.OutputFormat newOutputFormat;
  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapreduce.RecordWriter newRecordWriter;

  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapred.OutputFormat oldOutputFormat;
  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapred.RecordWriter oldRecordWriter;

  protected TezCounter outputRecordCounter;

  @VisibleForTesting
  TaskAttemptContext newApiTaskAttemptContext;
  @VisibleForTesting
  org.apache.hadoop.mapred.TaskAttemptContext oldApiTaskAttemptContext;

  @VisibleForTesting
  boolean isMapperOutput;

  protected OutputCommitter committer;

  public MROutput(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
  }

  @Override
  public List<Event> initialize() throws IOException, InterruptedException {
    List<Event> events = initializeBase();
    initWriter();
    return events;
  }

  protected List<Event> initializeBase() throws IOException, InterruptedException {
    getContext().requestInitialMemory(0l, null); //mandatory call
    taskNumberFormat.setMinimumIntegerDigits(5);
    taskNumberFormat.setGroupingUsed(false);
    nonTaskNumberFormat.setMinimumIntegerDigits(3);
    nonTaskNumberFormat.setGroupingUsed(false);
    UserPayload userPayload = getContext().getUserPayload();
    this.jobConf = new JobConf(getContext().getContainerConfiguration());
    TezUtils.addToConfFromByteString(this.jobConf, ByteString.copyFrom(userPayload.getPayload()));
    // Add tokens to the jobConf - in case they are accessed within the RW / OF
    jobConf.getCredentials().mergeAll(UserGroupInformation.getCurrentUser().getCredentials());
    this.isMapperOutput = jobConf.getBoolean(MRConfig.IS_MAP_PROCESSOR,
        false);
    if (this.isMapperOutput) {
      this.useNewApi = this.jobConf.getUseNewMapper();
    } else {
      this.useNewApi = this.jobConf.getUseNewReducer();
    }
    jobConf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
        getContext().getDAGAttemptNumber());
    TaskAttemptID taskAttemptId = org.apache.tez.mapreduce.hadoop.mapreduce.TaskAttemptContextImpl
        .createMockTaskAttemptID(getContext().getApplicationId().getClusterTimestamp(),
            getContext().getTaskVertexIndex(), getContext().getApplicationId().getId(),
            getContext().getTaskIndex(), getContext().getTaskAttemptNumber(), isMapperOutput);
    jobConf.set(JobContext.TASK_ATTEMPT_ID, taskAttemptId.toString());
    jobConf.set(JobContext.TASK_ID, taskAttemptId.getTaskID().toString());
    jobConf.setBoolean(JobContext.TASK_ISMAP, isMapperOutput);
    jobConf.setInt(JobContext.TASK_PARTITION,
      taskAttemptId.getTaskID().getId());
    jobConf.set(JobContext.ID, taskAttemptId.getJobID().toString());
    
    String outputFormatClassName;

    outputRecordCounter = getContext().getCounters().findCounter(
        TaskCounter.OUTPUT_RECORDS);

    if (useNewApi) {
      // set the output part name to have a unique prefix
      if (jobConf.get(MRJobConfig.FILEOUTPUTFORMAT_BASE_OUTPUT_NAME) == null) {
        jobConf.set(MRJobConfig.FILEOUTPUTFORMAT_BASE_OUTPUT_NAME,
            getOutputFileNamePrefix());
      }

      newApiTaskAttemptContext = createTaskAttemptContext(taskAttemptId);
      try {
        newOutputFormat =
            org.apache.hadoop.util.ReflectionUtils.newInstance(
                newApiTaskAttemptContext.getOutputFormatClass(), jobConf);
        outputFormatClassName = newOutputFormat.getClass().getName();
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }

      initCommitter(jobConf, useNewApi);
    } else {
      oldApiTaskAttemptContext =
          new org.apache.tez.mapreduce.hadoop.mapred.TaskAttemptContextImpl(
              jobConf, taskAttemptId,
              new MRTaskReporter(getContext()));
      oldOutputFormat = jobConf.getOutputFormat();
      outputFormatClassName = oldOutputFormat.getClass().getName();

      initCommitter(jobConf, useNewApi);
    }

    LOG.info(getContext().getDestinationVertexName() + ": "
        + "outputFormat=" + outputFormatClassName
        + ", using newmapreduce API=" + useNewApi);
    return null;
  }

  private void initWriter() throws IOException {
    if (useNewApi) {
      try {
        newRecordWriter =
            newOutputFormat.getRecordWriter(newApiTaskAttemptContext);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while creating record writer", e);
      }
    } else {
      FileSystem fs = FileSystem.get(jobConf);
      String finalName = getOutputName(getOutputFileNamePrefix());
      oldRecordWriter = oldOutputFormat.getRecordWriter(
          fs, jobConf, finalName, new MRReporter(getContext().getCounters()));
    }
  }

  @Override
  public void start() {
  }

  public void initCommitter(JobConf job, boolean useNewApi)
      throws IOException, InterruptedException {

    if (useNewApi) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("using new api for output committer");
      }

      this.committer = newOutputFormat.getOutputCommitter(
          newApiTaskAttemptContext);
    } else {
      this.committer = job.getOutputCommitter();
    }

    Path outputPath = FileOutputFormat.getOutputPath(job);
    if (outputPath != null) {
      if ((this.committer instanceof FileOutputCommitter)) {
        FileOutputFormat.setWorkOutputPath(job,
            ((FileOutputCommitter) this.committer).getTaskAttemptPath(
                oldApiTaskAttemptContext));
      } else {
        FileOutputFormat.setWorkOutputPath(job, outputPath);
      }
    }
    if (useNewApi) {
      this.committer.setupTask(newApiTaskAttemptContext);
    } else {
      this.committer.setupTask(oldApiTaskAttemptContext);
    }
  }

  public boolean isCommitRequired() throws IOException {
    if (useNewApi) {
      return committer.needsTaskCommit(newApiTaskAttemptContext);
    } else {
      return committer.needsTaskCommit(oldApiTaskAttemptContext);
    }
  }

  private TaskAttemptContext createTaskAttemptContext(TaskAttemptID attemptId) {
    return new TaskAttemptContextImpl(this.jobConf, attemptId, getContext().getCounters(),
        isMapperOutput, null);
  }

  protected String getOutputFileNamePrefix() {
    String prefix = jobConf.get(MRJobConfig.MROUTPUT_FILE_NAME_PREFIX);
    if (prefix == null) {
      prefix = "part-v" + 
          nonTaskNumberFormat.format(getContext().getTaskVertexIndex()) +  
          "-o" + nonTaskNumberFormat.format(getContext().getOutputIndex());
    }
    return prefix;
  }

  protected String getOutputName(String prefix) {
    // give a unique prefix to the output name
    return prefix + "-" + taskNumberFormat.format(getContext().getTaskIndex());
  }

  /**
   * Get a key value write to write Map Reduce compatible output
   */
  @Override
  public KeyValueWriter getWriter() throws IOException {
    return new KeyValueWriter() {
      private final boolean useNewWriter = useNewApi;

      @SuppressWarnings("unchecked")
      @Override
      public void write(Object key, Object value) throws IOException {
        if (useNewWriter) {
          try {
            newRecordWriter.write(key, value);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOInterruptedException("Interrupted while writing next key-value",e);
          }
        } else {
          oldRecordWriter.write(key, value);
        }
        outputRecordCounter.increment(1);
        getContext().notifyProgress();
      }
    };
  }

  @Override
  public void handleEvents(List<Event> outputEvents) {
    // Not expecting any events at the moment.
  }

  @Override
  public synchronized List<Event> close() throws IOException {
    flush();
    LOG.info(getContext().getDestinationVertexName() + " closed");
    long outputRecords = getContext().getCounters()
        .findCounter(TaskCounter.OUTPUT_RECORDS).getValue();
    getContext().getStatisticsReporter().reportItemsProcessed(outputRecords);

    return null;
  }
  
  /**
   * Call this in the processor before finishing to ensure outputs that 
   * outputs have been flushed. Must be called before commit.
   * @throws IOException
   */
  public void flush() throws IOException {
    if (flushed.getAndSet(true)) {
      return;
    }

    if (useNewApi) {
      try {
        newRecordWriter.close(newApiTaskAttemptContext);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while closing record writer", e);
      }
    } else {
      oldRecordWriter.close(null);
    }
  }

  /**
   * MROutput expects that a Processor call commit prior to the
   * Processor's completion
   * @throws IOException
   */
  public void commit() throws IOException {
    flush();
    if (useNewApi) {
      committer.commitTask(newApiTaskAttemptContext);
    } else {
      committer.commitTask(oldApiTaskAttemptContext);
    }
  }


  /**
   * MROutput expects that a Processor call abort in case of any error
   * ( including an error during commit ) prior to the Processor's completion
   * @throws IOException
   */
  public void abort() throws IOException {
    flush();
    if (useNewApi) {
      committer.abortTask(newApiTaskAttemptContext);
    } else {
      committer.abortTask(oldApiTaskAttemptContext);
    }
  }

}
