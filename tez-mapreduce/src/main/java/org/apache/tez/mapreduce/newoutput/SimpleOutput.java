package org.apache.tez.mapreduce.newoutput;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.KVWriter;
import org.apache.tez.engine.newapi.LogicalOutput;
import org.apache.tez.engine.newapi.TezOutputContext;
import org.apache.tez.engine.newapi.Writer;
import org.apache.tez.mapreduce.common.Utils;
import org.apache.tez.mapreduce.hadoop.MRConfig;
import org.apache.tez.mapreduce.hadoop.newmapred.MRReporter;
import org.apache.tez.mapreduce.hadoop.newmapreduce.TaskAttemptContextImpl;

public class SimpleOutput implements LogicalOutput {

  private static final Log LOG = LogFactory.getLog(SimpleOutput.class);

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  private TezOutputContext outputContext;

  private JobConf jobConf;

  boolean useNewApi;

  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapreduce.OutputFormat newOutputFormat;
  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapreduce.RecordWriter newRecordWriter;

  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapred.OutputFormat oldOutputFormat;
  @SuppressWarnings("rawtypes")
  org.apache.hadoop.mapred.RecordWriter oldRecordWriter;

  private TezCounter outputRecordCounter;
  private TezCounter fileOutputByteCounter;
  private List<Statistics> fsStats;

  private org.apache.hadoop.mapreduce.TaskAttemptContext taskAttemptContext;

  private boolean isMapperOutput;

  @Override
  public List<Event> initialize(TezOutputContext outputContext)
      throws IOException {
    LOG.info("Initializing Simple Output");
    this.outputContext = outputContext;
    Configuration conf = TezUtils.createConfFromUserPayload(
        outputContext.getUserPayload());
    this.jobConf = new JobConf(conf);
    this.useNewApi = this.jobConf.getUseNewMapper();
    this.isMapperOutput = jobConf.getBoolean(MRConfig.IS_MAP_PROCESSOR,
        false);

    outputRecordCounter = outputContext.getCounters().findCounter(
        TaskCounter.MAP_OUTPUT_RECORDS);
    fileOutputByteCounter = outputContext.getCounters().findCounter(
        FileOutputFormatCounter.BYTES_WRITTEN);

    if (useNewApi) {
      taskAttemptContext = createTaskAttemptContext();
      try {
        newOutputFormat =
            ReflectionUtils.newInstance(
                taskAttemptContext.getOutputFormatClass(), jobConf);
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }

      List<Statistics> matchedStats = null;
      if (newOutputFormat instanceof
          org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
        matchedStats =
            Utils.getFsStatistics(
                org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
                    .getOutputPath(taskAttemptContext),
                jobConf);
      }
      fsStats = matchedStats;

      long bytesOutPrev = getOutputBytes();
      try {
        newRecordWriter =
            newOutputFormat.getRecordWriter(taskAttemptContext);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while creating record writer", e);
      }
      long bytesOutCurr = getOutputBytes();
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    } else {
      oldOutputFormat = jobConf.getOutputFormat();

      List<Statistics> matchedStats = null;
      if (oldOutputFormat instanceof org.apache.hadoop.mapred.FileOutputFormat) {
        matchedStats =
            Utils.getFsStatistics(
                org.apache.hadoop.mapred.FileOutputFormat.getOutputPath(
                    jobConf),
                jobConf);
      }
      fsStats = matchedStats;

      FileSystem fs = FileSystem.get(jobConf);
      String finalName = getOutputName();

      long bytesOutPrev = getOutputBytes();
      oldRecordWriter =
          oldOutputFormat.getRecordWriter(
              fs, jobConf, finalName, new MRReporter(outputContext));
      long bytesOutCurr = getOutputBytes();
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    LOG.info("Initialized Simple Output"
        + ", using_new_api" + useNewApi);
    return null;
  }

  private TaskAttemptContext createTaskAttemptContext() {
    return new TaskAttemptContextImpl(this.jobConf, outputContext,
        isMapperOutput);
  }

  private long getOutputBytes() {
    if (fsStats == null) return 0;
    long bytesWritten = 0;
    for (Statistics stat: fsStats) {
      bytesWritten = bytesWritten + stat.getBytesWritten();
    }
    return bytesWritten;
  }

  private String getOutputName() {
    return "part-" + NUMBER_FORMAT.format(outputContext.getTaskIndex());
  }

  @Override
  public Writer getWriter() throws IOException {
    return new KVWriter() {
      private final boolean useNewWriter = useNewApi;

      @SuppressWarnings("unchecked")
      @Override
      public void write(Object key, Object value) throws IOException {
        long bytesOutPrev = getOutputBytes();
        if (useNewWriter) {
          try {
            newRecordWriter.write(key, value);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while writing next key-value",e);
          }
        } else {
          oldRecordWriter.write(key, value);
        }

        long bytesOutCurr = getOutputBytes();
        fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
        outputRecordCounter.increment(1);
      }
    };
  }

  @Override
  public void handleEvents(List<Event> outputEvents) {
    // Not expecting any events at the moment.
  }

  @Override
  public List<Event> close() throws IOException {
    LOG.info("Closing Simple Output");
    long bytesOutPrev = getOutputBytes();
    if (useNewApi) {
      try {
        newRecordWriter.close(taskAttemptContext);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while closing record writer", e);
      }
    } else {
      oldRecordWriter.close(null);
    }
    long bytesOutCurr = getOutputBytes();
    fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    LOG.info("Closed Simple Output");
    return null;
  }

  @Override
  public void setNumPhysicalOutputs(int numOutputs) {
    // Nothing to do for now
  }

}
