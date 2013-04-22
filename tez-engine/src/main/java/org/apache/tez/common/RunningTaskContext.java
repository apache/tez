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

import java.io.IOException;

import javax.crypto.SecretKey;

import org.apache.hadoop.util.Progress;
import org.apache.tez.engine.api.Partitioner;
import org.apache.tez.engine.api.Processor;

public class RunningTaskContext {
  
  protected SecretKey jobTokenSecret;
  protected TezTaskReporter reporter;
  protected Partitioner partitioner;
  protected Processor combineProcessor;
  protected TezTaskStatus status;
  protected Progress progress = new Progress();

  public Progress getProgress() {
    return progress;
  }

  public void setJobTokenSecret(SecretKey jobTokenSecret) {
    this.jobTokenSecret = jobTokenSecret;
  }

  public TezTaskStatus getStatus() {
    return status;
  }

  public TezTaskReporter getTaskReporter() {
    return reporter;
  }

  // TODO Doesn't belong here.
  public Processor getCombineProcessor() {
    return combineProcessor;
  }

  // TODO Doesn't belong here.
  public Partitioner getPartitioner() {
    return partitioner;
  }

  // TODO Doesn't belong here.
  public SecretKey getJobTokenSecret() {
    return jobTokenSecret;
  }
  
  public void statusUpdate() throws IOException, InterruptedException {
  }
}
