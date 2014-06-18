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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.mapreduce.input.base.MRInputBase;
import org.apache.tez.mapreduce.lib.MRInputUtils;
import org.apache.tez.mapreduce.lib.MRReader;
import org.apache.tez.mapreduce.lib.MRReaderMapReduce;
import org.apache.tez.mapreduce.lib.MRReaderMapred;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.events.RootInputDataInformationEvent;
import org.apache.tez.runtime.library.api.KeyValueReader;

public class MultiMRInput extends MRInputBase {

  private static final Log LOG = LogFactory.getLog(MultiMRInput.class);

  @Override
  public int getNumPhysicalInputs() {
    return super.getNumPhysicalInputs();
  }

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();
  private final AtomicInteger eventCount = new AtomicInteger(0);

  private List<MRReader> readers = new LinkedList<MRReader>();

  @Override
  public List<Event> initialize() throws IOException {
    super.initialize();
    LOG.info("Using New mapreduce API: " + useNewApi + ", numPhysicalInputs: "
        + getNumPhysicalInputs());
    return null;
  }

  public Collection<KeyValueReader> getKeyValueReaders() throws InterruptedException, IOException {
    lock.lock();
    try {
      while (eventCount.get() != getNumPhysicalInputs()) {
        condition.await();
      }
    } finally {
      lock.unlock();
    }
    return Collections
        .unmodifiableCollection(Lists.transform(readers, new Function<MRReader, KeyValueReader>() {
          @Override
          public KeyValueReader apply(MRReader input) {
            return input;
          }
        }));
  }

  @Override
  public Reader getReader() throws Exception {
    throw new UnsupportedOperationException("getReader not supported. use getKeyValueReaders");
  }

  @Override
  public void handleEvents(List<Event> inputEvents) throws Exception {
    lock.lock();
    try {
      Preconditions.checkState(eventCount.get() + inputEvents.size() <= getNumPhysicalInputs(),
          "Unexpected event. All physical sources already initialized");
      for (Event event : inputEvents) {
        MRReader reader = initFromEvent((RootInputDataInformationEvent) event);
        readers.add(reader);
        if (eventCount.incrementAndGet() == getNumPhysicalInputs()) {
          getContext().inputIsReady();
          condition.signal();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private MRReader initFromEvent(RootInputDataInformationEvent event) throws IOException {
    Preconditions.checkState(event != null, "Event must be specified");
    LOG.info("Initializing Reader: " + eventCount.get());
    MRSplitProto splitProto = MRSplitProto.parseFrom(event.getUserPayload());
    Object split = null;
    MRReader reader = null;
    JobConf localJobConf = new JobConf(jobConf);
    if (useNewApi) {
      split = MRInputUtils.getNewSplitDetailsFromEvent(splitProto, localJobConf);
      reader = new MRReaderMapReduce(localJobConf, (org.apache.hadoop.mapreduce.InputSplit) split,
          getContext().getCounters(), inputRecordCounter, getContext().getApplicationId()
          .getClusterTimestamp(), getContext().getTaskVertexIndex(), getContext()
          .getApplicationId().getId(), getContext().getTaskIndex(), getContext()
          .getTaskAttemptNumber());
      LOG.info("Split Details -> SplitClass: " + split.getClass().getName() + ", NewSplit: "
          + split);

    } else {
      split = MRInputUtils.getOldSplitDetailsFromEvent(splitProto, localJobConf);
      reader = new MRReaderMapred(localJobConf, (org.apache.hadoop.mapred.InputSplit) split,
          getContext().getCounters(), inputRecordCounter);
      LOG.info("Split Details -> SplitClass: " + split.getClass().getName() + ", OldSplit: "
          + split);
    }
    LOG.info("Initialized RecordReader from event");
    return reader;
  }

  @Override
  public List<Event> close() throws Exception {
    for (MRReader reader : readers) {
      reader.close();
    }
    return null;
  }

  @Override
  public void start() throws Exception {
    Preconditions.checkState(getNumPhysicalInputs() >= 1, "Expecting one or more physical inputs");
  }
}
