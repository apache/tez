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

package org.apache.tez.runtime.library.common.task.local.output;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

/**
 * Manipulate the working area for the transient store for maps and reduces.
 *
 * This class is used by map and reduce tasks to identify the directories that
 * they need to write to/read from for intermediate files. The callers of
 * these methods are from child space and see mapreduce.cluster.local.dir as
 * taskTracker/jobCache/jobId/attemptId
 * This class should not be used from TaskTracker space.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TezTaskOutputFiles extends TezTaskOutput {
  
  public TezTaskOutputFiles(Configuration conf, String uniqueId) {
    super(conf, uniqueId);
  }

  private static final Log LOG = LogFactory.getLog(TezTaskOutputFiles.class);

  private static final String SPILL_FILE_PATTERN = "%s_spill_%d.out";
  private static final String SPILL_INDEX_FILE_PATTERN = SPILL_FILE_PATTERN
      + ".index";

  

  // assume configured to $localdir/usercache/$user/appcache/$appId
  private LocalDirAllocator lDirAlloc =
    new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
  

  private Path getAttemptOutputDir() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("getAttemptOutputDir: "
          + Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR + "/"
          + uniqueId);
    }
    return new Path(Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR, uniqueId);
  }

  /**
   * Return the path to local map output file created earlier
   *
   * @return path
   * @throws IOException
   */
  public Path getOutputFile() throws IOException {
    Path attemptOutput =
      new Path(getAttemptOutputDir(), Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
    return lDirAlloc.getLocalPathToRead(attemptOutput.toString(), conf);
  }

  /**
   * Create a local map output file name.
   *
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public Path getOutputFileForWrite(long size) throws IOException {
    Path attemptOutput =
      new Path(getAttemptOutputDir(), Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptOutput.toString(), size, conf);
  }

  /**
   * Create a local map output file name. This should *only* be used if the size
   * of the file is not known. Otherwise use the equivalent which accepts a size
   * parameter.
   * 
   * @return path
   * @throws IOException
   */
  public Path getOutputFileForWrite() throws IOException {
    Path attemptOutput =
      new Path(getAttemptOutputDir(), Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptOutput.toString(), conf);
  }

  /**
   * Create a local map output file name on the same volume.
   */
  public Path getOutputFileForWriteInVolume(Path existing) {
    Path outputDir = new Path(existing.getParent(), Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR);
    Path attemptOutputDir = new Path(outputDir, uniqueId);
    return new Path(attemptOutputDir, Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
  }

  /**
   * Return the path to a local map output index file created earlier
   *
   * @return path
   * @throws IOException
   */
  public Path getOutputIndexFile() throws IOException {
    Path attemptIndexOutput =
      new Path(getAttemptOutputDir(), Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING +
                                      Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);
    return lDirAlloc.getLocalPathToRead(attemptIndexOutput.toString(), conf);
  }

  /**
   * Create a local map output index file name.
   *
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public Path getOutputIndexFileForWrite(long size) throws IOException {
    Path attemptIndexOutput =
      new Path(getAttemptOutputDir(), Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING +
                                      Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptIndexOutput.toString(),
        size, conf);
  }

  /**
   * Create a local map output index file name on the same volume.
   */
  public Path getOutputIndexFileForWriteInVolume(Path existing) {
    Path outputDir = new Path(existing.getParent(), Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR);
    Path attemptOutputDir = new Path(outputDir, uniqueId);
    return new Path(attemptOutputDir, Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING +
                                      Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);
  }

  /**
   * Return a local map spill file created earlier.
   *
   * @param spillNumber the number
   * @return path
   * @throws IOException
   */
  public Path getSpillFile(int spillNumber) throws IOException {
    return lDirAlloc.getLocalPathToRead(
        String.format(SPILL_FILE_PATTERN,
            uniqueId, spillNumber), conf);
  }

  /**
   * Create a local map spill file name.
   *
   * @param spillNumber the number
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(
        String.format(SPILL_FILE_PATTERN,
            uniqueId, spillNumber), size, conf);
  }

  /**
   * Return a local map spill index file created earlier
   *
   * @param spillNumber the number
   * @return path
   * @throws IOException
   */
  public Path getSpillIndexFile(int spillNumber) throws IOException {
    return lDirAlloc.getLocalPathToRead(
        String.format(SPILL_INDEX_FILE_PATTERN,
            uniqueId, spillNumber), conf);
  }

  /**
   * Create a local map spill index file name.
   *
   * @param spillNumber the number
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public Path getSpillIndexFileForWrite(int spillNumber, long size)
      throws IOException {
    return lDirAlloc.getLocalPathForWrite(
        String.format(SPILL_INDEX_FILE_PATTERN,
            uniqueId, spillNumber), size, conf);
  }

  /**
   * Return a local reduce input file created earlier
   *
   * @param attemptIdentifier an identifier for a task. The attempt information is ignored.
   * @return path
   * @throws IOException
   */
  public Path getInputFile(InputAttemptIdentifier attemptIdentifier) throws IOException {
    throw new UnsupportedOperationException("Incompatible with LocalRunner");
  }

  /**
   * Create a local reduce input file name.
   *
   * @param srcTaskId an identifier for a task
   * @param size the size of the file
   * @return path
   * @throws IOException
   */
  public Path getInputFileForWrite(int srcTaskId,
      long size) throws IOException {
    return lDirAlloc.getLocalPathForWrite(String.format(SPILL_FILE_PATTERN,
        uniqueId, srcTaskId),
        size, conf);
  }

  /** Removes all of the files related to a task. */
  public void removeAll() throws IOException {
    throw new UnsupportedOperationException("Incompatible with LocalRunner");
  }
}
