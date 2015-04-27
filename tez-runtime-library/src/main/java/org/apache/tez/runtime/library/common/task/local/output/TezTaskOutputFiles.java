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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.runtime.library.common.Constants;

/**
 * Manipulate the working area for the transient store for components in tez-runtime-library
 *
 * This class is used by Inputs and Outputs in tez-runtime-library to identify the directories
 * that they need to write to / read from for intermediate files.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TezTaskOutputFiles extends TezTaskOutput {
  
  public TezTaskOutputFiles(Configuration conf, String uniqueId) {
    super(conf, uniqueId);
  }

  private static final Logger LOG = LoggerFactory.getLogger(TezTaskOutputFiles.class);

  private static final String SPILL_FILE_DIR_PATTERN = "%s_%d";

  private static final String SPILL_FILE_PATTERN = "%s_src_%d_spill_%d.out";

  /*
  Under YARN, this defaults to one or more of the local directories, along with the appId in the path.
  Note: The containerId is not part of this.
  ${yarnLocalDir}/usercache/${user}/appcache/${applicationId}. (Referred to as ${appDir} later in the docs
   */
  private final LocalDirAllocator lDirAlloc =
    new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);

  /*
   * ${appDir}/output/${uniqueId}
   */
  private Path getAttemptOutputDir() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("getAttemptOutputDir: "
          + Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR + "/"
          + uniqueId);
    }
    return new Path(Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR, uniqueId);
  }


  /**
   * Create a local output file name.
   *
   * ${appDir}/output/${uniqueId}/file.out
   * e.g. application_1418684642047_0006/output/attempt_1418684642047_0006_1_00_000000_0_10003/file.out
   *
   * The structure of this file name is critical, to be served by the MapReduce ShuffleHandler.
   *
   * @param size the size of the file
   * @return path the path to write to
   * @throws IOException
   */
  @Override
  public Path getOutputFileForWrite(long size) throws IOException {
    Path attemptOutput =
      new Path(getAttemptOutputDir(), Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptOutput.toString(), size, conf);
  }

  /**
   * Create a local output file name. This should *only* be used if the size
   * of the file is not known. Otherwise use the equivalent which accepts a size
   * parameter.
   *
   * ${appDir}/output/${uniqueId}/file.out
   * e.g. application_1418684642047_0006/output/attempt_1418684642047_0006_1_00_000000_0_10003/file.out
   *
   * The structure of this file name is critical, to be served by the MapReduce ShuffleHandler.
   *
   * @return path the path to write to
   * @throws IOException
   */
  @Override
  public Path getOutputFileForWrite() throws IOException {
    Path attemptOutput =
      new Path(getAttemptOutputDir(), Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptOutput.toString(), conf);
  }

  /**
   * Create a local output file name on the same volume.
   * This is only meant to be used to rename temporary files to their final destination within the
   * same volume.
   *
   * ${appDir}/output/${uniqueId}/file.out
   * e.g.
   * existing:
   * application_1424502260528_0119/output/attempt_1424502260528_0119_1_07_000058_0_10012_0/file.out
   *
   * returnValue:
   * application_1424502260528_0119/output/attempt_1424502260528_0119_1_07_000058_0_10012/file.out
   *
   * The structure of this file name is critical, to be served by the MapReduce ShuffleHandler.
   *
   * @return path the path of the output file within the same volume
   */
  @Override
  public Path getOutputFileForWriteInVolume(Path existing) {
    //Get hold attempt directory (${appDir}/output/)
    Preconditions.checkArgument(existing.getParent().getParent() != null, "Parent directory's "
        + "parent can not be null");
    Path attemptDir = new Path(existing.getParent().getParent(), uniqueId);
    return new Path(attemptDir, Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
  }


  /**
   * Create a local output index file name.
   *
   * ${appDir}/output/${uniqueId}/file.out.index
   * e.g. application_1418684642047_0006/output/attempt_1418684642047_0006_1_00_000000_0_10003/file.out.index
   *
   * The structure of this file name is critical, to be served by the MapReduce ShuffleHandler.
   *
   * @param size the size of the file
   * @return path the path to write the index file to
   * @throws IOException
   */
  @Override
  public Path getOutputIndexFileForWrite(long size) throws IOException {
    Path attemptIndexOutput =
      new Path(getAttemptOutputDir(), Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING +
                                      Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);
    return lDirAlloc.getLocalPathForWrite(attemptIndexOutput.toString(),
        size, conf);
  }

  /**
   * Create a local output index file name on the same volume.
   * The intended usage of this method is to write the index file on the same volume as the
   * associated data file.
   *
   * ${appDir}/output/${uniqueId}/file.out.index
   * e.g.
   * existing:
   * application_1424502260528_0119/output/attempt_1424502260528_0119_1_07_000058_0_10012_0/file
   * .out.index
   *
   * returnValue:
   * application_1424502260528_0119/output/attempt_1424502260528_0119_1_07_000058_0_10012/file
   * .out.index
   *
   * The structure of this file name is critical, to be served by the MapReduce ShuffleHandler.
   */
  @Override
  public Path getOutputIndexFileForWriteInVolume(Path existing) {
    //Get hold attempt directory (${appDir}/output/)
    Preconditions.checkArgument(existing.getParent().getParent() != null, "Parent directory's "
        + "parent can not be null");
    Path attemptDir = new Path(existing.getParent().getParent(), uniqueId);
    return new Path(attemptDir, Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING
        + Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);
  }

  /**
   * Create a local spill file name.
   *
   * ${appDir}/output/${uniqueId}_${spillNumber}/file.out
   * e.g. application_1422270854961_0027/output/attempt_1422270854961_0027_1_00_000001_0_10003_1/file.out
   *
   * @param spillNumber the spill number
   * @param size the size of the spill file
   * @return path the path to write the spill file for the specific spillNumber
   * @throws IOException
   */
  @Override
  public Path getSpillFileForWrite(int spillNumber, long size)
      throws IOException {
    Preconditions.checkArgument(spillNumber >= 0, "Provide a valid spill number " + spillNumber);
    Path taskAttemptDir = new Path(Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR,
        String.format(SPILL_FILE_DIR_PATTERN, uniqueId, spillNumber));
    Path outputDir = new Path(taskAttemptDir, Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING);
    return lDirAlloc.getLocalPathForWrite(outputDir.toString(), size, conf);
  }

  /**
   * Create a local output spill index file name.
   *
   * ${appDir}/output/${uniqueId}_${spillNumber}/file.out.index
   * e.g. application_1422270854961_0027/output/attempt_1422270854961_0027_1_00_000001_0_10003_1/file.out.index
   *
   * @param spillNumber the spill number
   * @param size the size of the file
   * @return path the path to write the spill index file for the specific spillNumber
   * @throws IOException
   */
  @Override
  public Path getSpillIndexFileForWrite(int spillNumber, long size)
      throws IOException {
    Preconditions.checkArgument(spillNumber >= 0, "Provide a valid spill number " + spillNumber);
    Path taskAttemptDir = new Path(Constants.TEZ_RUNTIME_TASK_OUTPUT_DIR,
        String.format(SPILL_FILE_DIR_PATTERN, uniqueId, spillNumber));
    Path outputDir = new Path(taskAttemptDir, Constants.TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING +
        Constants.TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING);
    return lDirAlloc.getLocalPathForWrite(outputDir.toString(), size, conf);
  }


  /**
   * Create a local input file name.
   *
   * ${appDir}/${uniqueId}_src_{$srcId}_spill_${spillNumber}.out
   * e.g. application_1418684642047_0006/attempt_1418684642047_0006_1_00_000000_0_10004_src_10_spill_0.out
   *
   * Files are not clobbered due to the uniqueId along with spillId being different for Outputs /
   * Inputs within the same task (and across tasks)
   *
   * @param srcIdentifier The identifier for the source
   * @param spillNum
   * @param size the size of the file  @return path the path to the input file.
   * @throws IOException
   */
  @Override
  public Path getInputFileForWrite(int srcIdentifier,
      int spillNum, long size) throws IOException {
    return lDirAlloc.getLocalPathForWrite(getSpillFileName(srcIdentifier, spillNum), size, conf);
  }

  /**
   * Construct a spill file name, given a spill number and src id
   *
   * ${uniqueId}_src_${srcId}_spill_${spillNumber}.out
   * e.g. attempt_1418684642047_0006_1_00_000000_0_10004_src_10_spill_0.out
   *
   *
   * @param srcId
   * @param spillNum
   * @return a spill file name independent of the unique identifier and local directories
   */
  @Override
  public String getSpillFileName(int srcId, int spillNum) {
    return String.format(SPILL_FILE_PATTERN, uniqueId, srcId, spillNum);
  }
}
