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

package org.apache.tez.runtime.library.input;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.broadcast.input.BroadcastKVReader;
import org.apache.tez.runtime.library.broadcast.input.BroadcastShuffleManager;

import com.google.common.base.Preconditions;
public class ShuffledUnorderedKVInput implements LogicalInput {

  private static final Log LOG = LogFactory.getLog(ShuffledUnorderedKVInput.class);
  
  private Configuration conf;
  private int numInputs = -1;
  private BroadcastShuffleManager shuffleManager;
  private final BlockingQueue<Event> pendingEvents = new LinkedBlockingQueue<Event>();
  private volatile long firstEventReceivedTime = -1;
  @SuppressWarnings("rawtypes")
  private BroadcastKVReader kvReader;
  
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  
  public ShuffledUnorderedKVInput() {
  }

  @Override
  public List<Event> initialize(TezInputContext inputContext) throws Exception {
    Preconditions.checkArgument(numInputs != -1, "Number of Inputs has not been set");
    this.conf = TezUtils.createConfFromUserPayload(inputContext.getUserPayload());
    this.conf.setStrings(TezJobConfig.LOCAL_DIRS, inputContext.getWorkDirs());

    if (numInputs == 0) {
      return null;
    }

    this.shuffleManager = new BroadcastShuffleManager(inputContext, conf, numInputs);
    return Collections.emptyList();
  }

  @Override
  public void start() throws IOException {
    synchronized (this) {
      if (!isStarted.get()) {
        this.shuffleManager.run();
        this.kvReader = this.shuffleManager.createReader();
        List<Event> pending = new LinkedList<Event>();
        pendingEvents.drainTo(pending);
        if (pending.size() > 0) {
          LOG.info("NoAutoStart delay in processing first event: "
              + (System.currentTimeMillis() - firstEventReceivedTime));
          shuffleManager.handleEvents(pending);
        }
        isStarted.set(true);
      }
    }
  }

  @Override
  public KeyValueReader getReader() throws Exception {
    if (numInputs == 0) {
      return new KeyValueReader() {
        @Override
        public boolean next() throws IOException {
          return false;
        }

        @Override
        public Object getCurrentKey() throws IOException {
          throw new RuntimeException("No data available in Input");
        }

        @Override
        public Object getCurrentValue() throws IOException {
          throw new RuntimeException("No data available in Input");
        }
      };
    }
    return this.kvReader;
  }

  @Override
  public void handleEvents(List<Event> inputEvents) throws IOException {
    if (numInputs == 0) {
      throw new RuntimeException("No input events expected as numInputs is 0");
    }
    if (!isStarted.get()) {
      synchronized(this) {
        if (!isStarted.get()) {
          if (firstEventReceivedTime == -1) {
            firstEventReceivedTime = System.currentTimeMillis();
          }
          pendingEvents.addAll(inputEvents);
          return;
        }
      }
    }
    shuffleManager.handleEvents(inputEvents);
  }

  @Override
  public List<Event> close() throws Exception {
    if (numInputs != 0) {
      this.shuffleManager.shutdown();
    }
    return null;
  }

  @Override
  public void setNumPhysicalInputs(int numInputs) {
    this.numInputs = numInputs;
  }

}
