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

package org.apache.tez.mapreduce.hadoop.mapred;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.mapreduce.common.Utils;

public class MRReporter implements Reporter {

  private TezCounters tezCounters;
  private InputSplit split;
  private float progress = 0f;
  
  
  public MRReporter(TezCounters tezCounters) {
    this(tezCounters, null);
  }

  public MRReporter(TezCounters tezCounters, InputSplit split) {
    this.tezCounters = tezCounters;
    this.split = split;
  }
  
  @Override
  public void progress() {
    //TODO NEWTEZ
  }

  @Override
  public void setStatus(String status) {
    // Not setting status string in Tez.

  }

  @Override
  public Counter getCounter(Enum<?> name) {
    return Utils.getMRCounter(tezCounters.findCounter(name));
  }

  @Override
  public Counter getCounter(String group, String name) {
    return Utils.getMRCounter(tezCounters.findCounter(group,
        name));
  }

  @Override
  public void incrCounter(Enum<?> key, long amount) {
    getCounter(key).increment(amount);
  }

  @Override
  public void incrCounter(String group, String counter, long amount) {
    getCounter(group, counter).increment(amount);
  }

  @Override
  public InputSplit getInputSplit() throws UnsupportedOperationException {
    if (split == null) {
      throw new UnsupportedOperationException("Input only available on map");
    } else {
      return split;
    }
  }

  public void setProgress(float progress) {
    this.progress = progress;
  }
  
  @Override
  public float getProgress() {
    // TODO NEWTEZ This is likely broken. Only set on task complete in Map/ReduceProcessor
    return this.progress;
  }

}
