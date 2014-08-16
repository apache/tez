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

package org.apache.tez.common;

import java.io.File;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.log4j.FileAppender;
import org.apache.tez.dag.api.TezConstants;

/**
 * A simple log4j-appender for a tez container's logs.
 * 
 */
@Unstable
public class TezContainerLogAppender extends FileAppender {
  private String containerLogDir;
  private String logFileName = TezConstants.TEZ_CONTAINER_LOG_FILE_NAME;
  //so that log4j can configure it from the configuration(log4j.properties). 

  @Override
  public void activateOptions() {
    synchronized (this) {
      setFile(new File(this.containerLogDir, logFileName).toString());
      setAppend(true);
      super.activateOptions();
    }
  }

  /**
   * Set the name of the file for logging. This should NOT be an absolute path.
   * The file will be created within the container's log directory.
   * 
   * @param fileName
   */
  public void setLogFileName(String fileName) {
    if (fileName == null || fileName.contains(File.pathSeparator)) {
      throw new RuntimeException(
          "Invalid filename specified: "
              + fileName
              + " . FileName should not have a path component and should not be empty.");
    }
    this.logFileName = fileName;
  }

  public String getLogFileName() {
    return logFileName;
  }

  /**
   * Getter/Setter methods for log4j.
   */

  public String getContainerLogDir() {
    return this.containerLogDir;
  }

  public void setContainerLogDir(String containerLogDir) {
    this.containerLogDir = containerLogDir;
  }
}
