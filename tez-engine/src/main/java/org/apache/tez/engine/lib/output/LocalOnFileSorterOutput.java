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

package org.apache.tez.engine.lib.output;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.engine.common.task.local.output.TezTaskOutput;

public class LocalOnFileSorterOutput extends OnFileSortedOutput {

  private static final Log LOG = LogFactory.getLog(LocalOnFileSorterOutput.class);
  
  public LocalOnFileSorterOutput(TezEngineTaskContext task) throws IOException {
    super(task);
  }

  @Override
  public void close() throws IOException, InterruptedException {
    LOG.info("XXX close");

    super.close();


    TezTaskOutput mapOutputFile = sorter.getMapOutput();
    FileSystem localFs = FileSystem.getLocal(mapOutputFile.getConf());

    Path src = mapOutputFile.getOutputFile();
    Path dst = 
        mapOutputFile.getInputFileForWrite(
            sorter.getTaskAttemptId().getTaskID(),
            localFs.getFileStatus(src).getLen());

    localFs.rename(src, dst);
    LOG.info("XXX renaming src = " + src + ", dst = " + dst);
  }
}
