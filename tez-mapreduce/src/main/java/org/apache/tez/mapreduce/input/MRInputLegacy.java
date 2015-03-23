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

package org.apache.tez.mapreduce.input;

import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;

@LimitedPrivate("Hive")
public class MRInputLegacy extends MRInput {

  private static final Logger LOG = LoggerFactory.getLogger(MRInputLegacy.class);
  
  private InputDataInformationEvent initEvent;
  private volatile boolean inited = false;
  private ReentrantLock eventLock = new ReentrantLock();
  private Condition eventCondition = eventLock.newCondition();

  
  /**
   * Create an {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder}
   * @param conf Configuration for the {@link MRInputLegacy}
   * @param inputFormat InputFormat derived class
   * @return {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder}
   */
  public static MRInputConfigBuilder createConfigBuilder(Configuration conf, Class<?> inputFormat) {
    return MRInput.createConfigBuilder(conf, inputFormat).setInputClassName(MRInputLegacy.class.getName());
  }

  /**
   * Create an {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder} for a FileInputFormat
   * @param conf Configuration for the {@link MRInputLegacy}
   * @param inputFormat FileInputFormat derived class
   * @param inputPaths Comma separated input paths
   * @return {@link org.apache.tez.mapreduce.input.MRInput.MRInputConfigBuilder}
   */
  public static MRInputConfigBuilder createConfigBuilder(Configuration conf, Class<?> inputFormat,
                                                         String inputPaths) {
    return MRInput.createConfigBuilder(conf, inputFormat, inputPaths).setInputClassName(
        MRInputLegacy.class.getName());
  }
  
  public MRInputLegacy(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
  }

  @Private
  protected void initializeInternal() throws IOException {
    LOG.info("MRInputLegacy deferring initialization");
  }
  
  @Private
  public org.apache.hadoop.mapreduce.InputSplit getNewInputSplit() {
    return (org.apache.hadoop.mapreduce.InputSplit) mrReader.getSplit();
  }  

  @SuppressWarnings("rawtypes")
  @Unstable
  public org.apache.hadoop.mapreduce.RecordReader getNewRecordReader() {
    return (org.apache.hadoop.mapreduce.RecordReader) mrReader.getRecordReader();
  }

  @Private
  public InputSplit getOldInputSplit() {
    return (InputSplit) mrReader.getSplit();
  }

  @Unstable
  public boolean isUsingNewApi() {
    return this.useNewApi;
  }

  @SuppressWarnings("rawtypes")
  @Private
  public RecordReader getOldRecordReader() {
    return (RecordReader) mrReader.getRecordReader();
  }
  
  @LimitedPrivate("hive")
  public void init() throws IOException {
    super.initializeInternal();
    checkAndAwaitRecordReaderInitialization();
  }
  
  @Override
  void processSplitEvent(InputDataInformationEvent event) {
    eventLock.lock();
    try {
      initEvent = event;
      // Don't process event, but signal in case init is waiting on the event.
      eventCondition.signal();
    } finally {
      eventLock.unlock();
    }
  }

  @Override
  void checkAndAwaitRecordReaderInitialization() throws IOException {
    eventLock.lock();
    try {
      if (inited) {
        return;
      }
      if (splitInfoViaEvents && !inited) {
        if (initEvent == null) {
          LOG.info("Awaiting init event before initializing record reader");
          try {
            eventCondition.await();
          } catch (InterruptedException e) {
            throw new IOException("Interrupted while awaiting init event", e);
          }
        }
        if (initEvent != null) {
          initFromEvent(initEvent);
          inited = true;
        } else {
          throw new IOException("Received a signal for init but init event is null");
        }
      } else {
        // Already inited
        return;
      }
    } finally {
      eventLock.unlock();
    }
  }
}
