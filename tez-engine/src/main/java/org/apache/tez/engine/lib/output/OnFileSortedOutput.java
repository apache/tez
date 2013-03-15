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

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.api.Master;
import org.apache.tez.api.Output;
import org.apache.tez.common.TezTask;
import org.apache.tez.engine.common.sort.SortingOutput;
import org.apache.tez.engine.common.sort.impl.ExternalSorter;
import org.apache.tez.engine.common.sort.impl.dflt.DefaultSorter;
import org.apache.tez.records.OutputContext;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

/**
 * {@link OnFileSortedOutput} is an {@link Output} which sorts key/value pairs 
 * written to it and persists it to a file.
 */
public class OnFileSortedOutput implements SortingOutput {
  
  protected ExternalSorter sorter;
  
  @Inject
  public OnFileSortedOutput(
      @Assisted TezTask task
      ) throws IOException {
    sorter = new DefaultSorter(task);
  }
  
  public void initialize(Configuration conf, Master master) 
      throws IOException, InterruptedException {
    sorter.initialize(conf, master);
  }

  public void setTask(TezTask task) {
    sorter.setTask(task);
  }
  
  public void write(Object key, Object value) throws IOException,
      InterruptedException {
    sorter.write(key, value);
  }

  public void close() throws IOException, InterruptedException {
    sorter.flush();
    sorter.close();
  }

  @Override
  public OutputContext getOutputContext() {
    return null;
  }

}
