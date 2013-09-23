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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.engine.common.task.local.output.TezTaskOutput;
import org.apache.tez.engine.newapi.Event;

public class LocalOnFileSorterOutput extends OnFileSortedOutput {

  private static final Log LOG = LogFactory.getLog(LocalOnFileSorterOutput.class);

  

  @Override
  public List<Event> close() throws IOException {
    LOG.debug("Closing LocalOnFileSorterOutput");
    super.close();

    TezTaskOutput mapOutputFile = sorter.getMapOutput();
    FileSystem localFs = FileSystem.getLocal(conf);

    Path src = mapOutputFile.getOutputFile();
    Path dst =
        mapOutputFile.getInputFileForWrite(
            outputContext.getTaskIndex(),
            localFs.getFileStatus(src).getLen());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Renaming src = " + src + ", dst = " + dst);
    }
    localFs.rename(src, dst);
    // TODO NEWTEZ Event generation.
    return null;
  }
}
