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
package org.apache.tez.engine.lib.input;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezTask;
import org.apache.tez.common.TezTaskReporter;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.common.combine.CombineInput;
import org.apache.tez.engine.common.shuffle.impl.Shuffle;
import org.apache.tez.engine.common.sort.impl.TezRawKeyValueIterator;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

/**
 * {@link ShuffledMergedInput} in an {@link Input} which shuffles intermediate
 * sorted data, merges them and provides key/<values> to the consumer. 
 */
public class ShuffledMergedInput implements Input {

  static final Log LOG = LogFactory.getLog(ShuffledMergedInput.class);
  TezRawKeyValueIterator rIter = null;

  private TezTask task;
  
  private Configuration conf;
  private CombineInput raw;

  @Inject
  public ShuffledMergedInput(
      @Assisted TezTask task
      ) {
  }

  public void setTask(TezTask task) {
    this.task = task;
  }
  
  public void initialize(Configuration conf, Master master) throws IOException,
      InterruptedException {
    this.conf = conf;
    
    Shuffle shuffle = 
      new Shuffle(
          task, this.conf,      
          this.conf.getInt(
              TezJobConfig.TEZ_ENGINE_TASK_INDEGREE, 
              TezJobConfig.DEFAULT_TEZ_ENGINE_TASK_INDEGREE),
          (TezTaskReporter)master, 
          task.getCombineProcessor());
    rIter = shuffle.run();
    
    raw = new CombineInput(rIter);
  }

  public boolean hasNext() throws IOException, InterruptedException {
    return raw.hasNext();
  }

  public Object getNextKey() throws IOException, InterruptedException {
    return raw.getNextKey();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public Iterable getNextValues() 
      throws IOException, InterruptedException {
    return raw.getNextValues();
  }

  public float getProgress() throws IOException, InterruptedException {
    return raw.getProgress();
  }

  public void close() throws IOException {
    raw.close();
  }

  public TezRawKeyValueIterator getIterator() {
    return rIter;
  }
  
}
