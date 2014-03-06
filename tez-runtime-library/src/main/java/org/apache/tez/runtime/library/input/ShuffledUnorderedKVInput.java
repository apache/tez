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
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.broadcast.input.BroadcastShuffleInputEventHandler;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.readers.ShuffledUnorderedKVReader;
import org.apache.tez.runtime.library.shuffle.common.impl.ShuffleManager;
import org.apache.tez.runtime.library.shuffle.common.impl.SimpleFetchedInputAllocator;

import com.google.common.base.Preconditions;
public class ShuffledUnorderedKVInput implements LogicalInput, MemoryUpdateCallback {

  private static final Log LOG = LogFactory.getLog(ShuffledUnorderedKVInput.class);
  
  private Configuration conf;
  private int numInputs = -1;
  private TezInputContext inputContext;
  private ShuffleManager shuffleManager;
  private final BlockingQueue<Event> pendingEvents = new LinkedBlockingQueue<Event>();
  private volatile long firstEventReceivedTime = -1;
  @SuppressWarnings("rawtypes")
  private ShuffledUnorderedKVReader kvReader;
  
  private final AtomicBoolean isStarted = new AtomicBoolean(false);
  private TezCounter inputRecordCounter;
  
  private SimpleFetchedInputAllocator inputManager;
  private BroadcastShuffleInputEventHandler inputEventHandler;
  
  private volatile long initialMemoryAvailable = -1;
  
  public ShuffledUnorderedKVInput() {
  }

  @Override
  public List<Event> initialize(TezInputContext inputContext) throws Exception {
    Preconditions.checkArgument(numInputs != -1, "Number of Inputs has not been set");
    this.inputContext = inputContext;
    this.conf = TezUtils.createConfFromUserPayload(inputContext.getUserPayload());

    if (numInputs == 0) {
      inputContext.requestInitialMemory(0l, null);
      isStarted.set(true);
      inputContext.inputIsReady();
      return Collections.emptyList();
    } else {
      long initalMemReq = getInitialMemoryReq();
      this.inputContext.requestInitialMemory(initalMemReq, this);
    }

    this.conf.setStrings(TezJobConfig.LOCAL_DIRS, inputContext.getWorkDirs());
    this.inputRecordCounter = inputContext.getCounters().findCounter(
        TaskCounter.INPUT_RECORDS_PROCESSED);
    return Collections.emptyList();
  }

  @Override
  public void start() throws IOException {
    synchronized (this) {
      if (!isStarted.get()) {
        ////// Initial configuration
        Preconditions.checkState(initialMemoryAvailable != -1,
            "Initial memory available must be configured before starting");
        CompressionCodec codec;
        if (ConfigUtils.isIntermediateInputCompressed(conf)) {
          Class<? extends CompressionCodec> codecClass = ConfigUtils
              .getIntermediateInputCompressorClass(conf, DefaultCodec.class);
          codec = ReflectionUtils.newInstance(codecClass, conf);
        } else {
          codec = null;
        }
        
        boolean ifileReadAhead = conf.getBoolean(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD,
            TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_DEFAULT);
        int ifileReadAheadLength = 0;
        int ifileBufferSize = 0;

        if (ifileReadAhead) {
          ifileReadAheadLength = conf.getInt(TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES,
              TezJobConfig.TEZ_RUNTIME_IFILE_READAHEAD_BYTES_DEFAULT);
        }
        ifileBufferSize = conf.getInt("io.file.buffer.size",
            TezJobConfig.TEZ_RUNTIME_IFILE_BUFFER_SIZE_DEFAULT);
        
        this.shuffleManager = new ShuffleManager(inputContext, conf, numInputs);
        
        this.inputManager = new SimpleFetchedInputAllocator(inputContext.getUniqueIdentifier(), conf,
            inputContext.getTotalMemoryAvailableToTask());
        inputManager.setInitialMemoryAvailable(initialMemoryAvailable);
        inputManager.configureAndStart();
        
        this.inputEventHandler = new BroadcastShuffleInputEventHandler(
            inputContext, shuffleManager, inputManager, codec, ifileReadAhead,
            ifileReadAheadLength);

        this.shuffleManager.setCompressionCodec(codec);
        this.shuffleManager.setIfileParameters(ifileBufferSize, ifileReadAhead, ifileReadAheadLength);
        this.shuffleManager.setFetchedInputAllocator(inputManager);
        this.shuffleManager.setInputEventHandler(inputEventHandler);        
        ////// End of Initial configuration

        this.shuffleManager.run();
        this.kvReader = createReader(inputRecordCounter, codec,
            ifileBufferSize, ifileReadAhead, ifileReadAheadLength);
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
          // This queue will keep growing if the Processor decides never to
          // start the event. The Input, however has no idea, on whether start
          // will be invoked or not.
          pendingEvents.addAll(inputEvents);
          return;
        }
      }
    }
    shuffleManager.handleEvents(inputEvents);
  }

  @Override
  public List<Event> close() throws Exception {
    if (this.shuffleManager != null) {
      this.shuffleManager.shutdown();
    }
    return null;
  }

  @Override
  public void setNumPhysicalInputs(int numInputs) {
    this.numInputs = numInputs;
  }

  @Override
  public void memoryAssigned(long assignedSize) {
    this.initialMemoryAvailable = assignedSize;
  }

  private long getInitialMemoryReq() {
    return SimpleFetchedInputAllocator.getInitialMemoryReq(conf,
        inputContext.getTotalMemoryAvailableToTask());
  }
  
  
  @SuppressWarnings("rawtypes")
  private ShuffledUnorderedKVReader createReader(TezCounter inputRecordCounter, CompressionCodec codec,
      int ifileBufferSize, boolean ifileReadAheadEnabled, int ifileReadAheadLength)
      throws IOException {
    return new ShuffledUnorderedKVReader(shuffleManager, conf, codec, ifileReadAheadEnabled,
        ifileReadAheadLength, ifileBufferSize, inputRecordCounter);
  }

}