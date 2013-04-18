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

package org.apache.tez.mapreduce.processor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tez.common.TezTaskReporter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.hadoop.mapred.MRCounters;

@InterfaceAudience.Private
@InterfaceStability.Unstable 
public class MRTaskReporter 
    extends org.apache.hadoop.mapreduce.StatusReporter
    implements Reporter {

  private final TezTaskReporterImpl reporter;
  
  private InputSplit split = null;

  public MRTaskReporter(TezTaskReporter reporter) {
    this.reporter =  (TezTaskReporterImpl)reporter;
  }

  // getters and setters for flag
  void setProgressFlag() {
    reporter.setProgressFlag();
  }
  boolean resetProgressFlag() {
    return reporter.resetProgressFlag();
  }
  public void setStatus(String status) {
    reporter.setStatus(status);
  }
  public void setProgress(float progress) {
    reporter.setProgress(progress);
  }
  
  public float getProgress() {
    return reporter.getProgress();
  };
  
  public void progress() {
    reporter.progress();
  }
  
  public Counters.Counter getCounter(String group, String name) {
    TezCounter counter = reporter.getCounter(group, name);
    MRCounters.MRCounter mrCounter = null;
    if (counter != null) {
      mrCounter = new MRCounters.MRCounter(counter);
    }
    return mrCounter;
  }
  
  public Counters.Counter getCounter(Enum<?> name) {
    TezCounter counter = reporter.getCounter(name);
    MRCounters.MRCounter mrCounter = null;
    if (counter != null) {
      mrCounter = new MRCounters.MRCounter(counter);
    }
    return mrCounter;
  }
  
  public void incrCounter(Enum<?> key, long amount) {
    reporter.incrCounter(key, amount);
  }
  
  public void incrCounter(String group, String counter, long amount) {
    reporter.incrCounter(group, counter, amount);
  }
  
  public void setInputSplit(InputSplit split) {
    this.split = split;
  }
  
  public InputSplit getInputSplit() throws UnsupportedOperationException {
    if (split == null) {
      throw new UnsupportedOperationException("Input only available on map");
    } else {
      return split;
    }
  }  
  
  public void startCommunicationThread() {
    reporter.startCommunicationThread();
  }
  
  public void stopCommunicationThread() throws InterruptedException {
    reporter.stopCommunicationThread();
  }
}
