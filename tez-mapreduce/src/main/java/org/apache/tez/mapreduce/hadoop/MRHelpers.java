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

package org.apache.tez.mapreduce.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.dag.api.VertexLocationHint.TaskLocationHint;

public class MRHelpers {

  private static final Log LOG = LogFactory.getLog(MRHelpers.class);

  /**
   * Comparator for org.apache.hadoop.mapreduce.InputSplit
   */
  private static class InputSplitComparator
      implements Comparator<org.apache.hadoop.mapreduce.InputSplit> {
    @Override
    public int compare(org.apache.hadoop.mapreduce.InputSplit o1,
        org.apache.hadoop.mapreduce.InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          return 0;
        } else {
          return -1;
        }
      } catch (IOException ie) {
        throw new RuntimeException("exception in InputSplit compare", ie);
      } catch (InterruptedException ie) {
        throw new RuntimeException("exception in InputSplit compare", ie);
      }
    }
  }

  /**
   * Comparator for org.apache.hadoop.mapred.InputSplit
   */
  private static class OldInputSplitComparator
      implements Comparator<org.apache.hadoop.mapred.InputSplit> {
    @Override
    public int compare(org.apache.hadoop.mapred.InputSplit o1,
        org.apache.hadoop.mapred.InputSplit o2) {
      try {
        long len1 = o1.getLength();
        long len2 = o2.getLength();
        if (len1 < len2) {
          return 1;
        } else if (len1 == len2) {
          return 0;
        } else {
          return -1;
        }
      } catch (IOException ie) {
        throw new RuntimeException("Problem getting input split size", ie);
      }
    }
  }

  /**
   * Generate new-api mapreduce InputFormat splits
   * @param jobContext JobContext required by InputFormat
   * @param inputSplitDir Directory in which to generate splits information
   *
   * @return InputSplitInfo containing the split files' information and the
   * location hints for each split generated to be used to determining parallelism of
   * the map stage.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  private static InputSplitInfo writeNewSplits(JobContext jobContext,
      Path inputSplitDir) throws IOException, InterruptedException,
      ClassNotFoundException {
    Configuration conf = jobContext.getConfiguration();
    InputFormat<?, ?> input =
        ReflectionUtils.newInstance(jobContext.getInputFormatClass(), conf);

    List<org.apache.hadoop.mapreduce.InputSplit> array =
        input.getSplits(jobContext);
    org.apache.hadoop.mapreduce.InputSplit[] splits =
        (org.apache.hadoop.mapreduce.InputSplit[])
        array.toArray(
            new org.apache.hadoop.mapreduce.InputSplit[array.size()]);

    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits, new InputSplitComparator());

    JobSplitWriter.createSplitFiles(inputSplitDir, conf,
        inputSplitDir.getFileSystem(conf), splits);

    TaskLocationHint[] locationHints =
        new TaskLocationHint[splits.length];
    for (int i = 0; i < splits.length; ++i) {
      locationHints[i] = new TaskLocationHint(splits[i].getLocations(), null);
    }

    return new InputSplitInfo(
        JobSubmissionFiles.getJobSplitFile(inputSplitDir),
        JobSubmissionFiles.getJobSplitMetaFile(inputSplitDir),
        splits.length, locationHints);
  }

  /**
   * Generate old-api mapred InputFormat splits
   * @param jobConf JobConf required by InputFormat class
   * @param inputSplitDir Directory in which to generate splits information
   *
   * @return InputSplitInfo containing the split files' information and the
   * number of splits generated to be used to determining parallelism of
   * the map stage.
   *
   * @throws IOException
   */
  private static InputSplitInfo writeOldSplits(JobConf jobConf,
      Path inputSplitDir) throws IOException {
    org.apache.hadoop.mapred.InputSplit[] splits =
        jobConf.getInputFormat().getSplits(jobConf, jobConf.getNumMapTasks());
    // sort the splits into order based on size, so that the biggest
    // go first
    Arrays.sort(splits, new OldInputSplitComparator());
    JobSplitWriter.createSplitFiles(inputSplitDir, jobConf,
        inputSplitDir.getFileSystem(jobConf), splits);

    TaskLocationHint[] locationHints =
        new TaskLocationHint[splits.length];
    for (int i = 0; i < splits.length; ++i) {
      locationHints[i] = new TaskLocationHint(splits[i].getLocations(), null);
    }

    return new InputSplitInfo(
        JobSubmissionFiles.getJobSplitFile(inputSplitDir),
        JobSubmissionFiles.getJobSplitMetaFile(inputSplitDir),
        splits.length, locationHints);
  }

  /**
   * Helper api to generate splits
   * @param conf Configuration with all necessary information set to generate
   * splits. The following are required at a minimum:
   *
   *   - mapred.mapper.new-api: determine whether mapred.InputFormat or
   *     mapreduce.InputFormat is to be used
   *   - mapred.input.format.class or mapreduce.job.inputformat.class:
   *     determines the InputFormat class to be used
   *
   * In addition to this, all the configs needed by the InputFormat class also
   * have to be set. For example, FileInputFormat needs the input directory
   * paths to be set in the config.
   *
   * @param inputSplitsDir Directory in which the splits file and meta info file
   * will be generated. job.split and job.splitmetainfo files in this directory
   * will be overwritten.
   *
   * @return InputSplitInfo containing the split files' information and the
   * number of splits generated to be used to determining parallelism of
   * the map stage.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   */
  public static InputSplitInfo generateInputSplits(Configuration conf,
      Path inputSplitsDir) throws IOException, InterruptedException,
      ClassNotFoundException {
    Job job = Job.getInstance(conf);
    JobConf jobConf = new JobConf(conf);
    if (jobConf.getUseNewMapper()) {
      LOG.info("Generating new input splits"
          + ", splitsDir=" + inputSplitsDir.toString());
      return writeNewSplits(job, inputSplitsDir);
    } else {
      LOG.info("Generating old input splits"
          + ", splitsDir=" + inputSplitsDir.toString());
      return writeOldSplits(jobConf, inputSplitsDir);
    }
  }

}
