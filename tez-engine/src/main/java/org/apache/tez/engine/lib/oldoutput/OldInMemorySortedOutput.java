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
package org.apache.tez.engine.lib.oldoutput;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.RunningTaskContext;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.common.sort.SortingOutput;
import org.apache.tez.engine.records.OutputContext;

/**
 * {@link OldInMemorySortedOutput} is an {@link Output} which sorts key/value pairs 
 * written to it and persists it to a file.
 */
public class OldInMemorySortedOutput implements SortingOutput {
  
  public OldInMemorySortedOutput(TezEngineTaskContext task) throws IOException {
  }
  
  public void initialize(Configuration conf, Master master) 
      throws IOException, InterruptedException {
  }

  public void setTask(RunningTaskContext task) {
  }
  
  public void write(Object key, Object value) throws IOException,
      InterruptedException {
  }

  public void close() throws IOException, InterruptedException {
  }

  @Override
  public OutputContext getOutputContext() {
    return null;
  }

}
