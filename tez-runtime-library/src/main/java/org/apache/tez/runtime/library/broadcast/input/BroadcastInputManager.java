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

package org.apache.tez.runtime.library.broadcast.input;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.apache.tez.runtime.library.shuffle.common.DiskFetchedInput;
import org.apache.tez.runtime.library.shuffle.common.FetchedInput;
import org.apache.tez.runtime.library.shuffle.common.FetchedInputAllocator;
import org.apache.tez.runtime.library.shuffle.common.FetchedInputCallback;
import org.apache.tez.runtime.library.shuffle.common.MemoryFetchedInput;

public class BroadcastInputManager implements FetchedInputAllocator,
    FetchedInputCallback {

  private static final Log LOG = LogFactory.getLog(BroadcastInputManager.class);
  
  private final Configuration conf;

  private final TezTaskOutputFiles fileNameAllocator;
  private final LocalDirAllocator localDirAllocator;

  // Configuration parameters
  private final long memoryLimit;
  private final long maxSingleShuffleLimit;

  private volatile long usedMemory = 0;

  public BroadcastInputManager(String uniqueIdentifier, Configuration conf) {
    this.conf = conf;

    this.fileNameAllocator = new TezTaskOutputFiles(conf,
        uniqueIdentifier);
    this.localDirAllocator = new LocalDirAllocator(TezJobConfig.LOCAL_DIRS);

    // Setup configuration
    final float maxInMemCopyUse = conf.getFloat(
        TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT,
        TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for "
          + TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT + ": "
          + maxInMemCopyUse);
    }

    // Allow unit tests to fix Runtime memory
    this.memoryLimit = (long) (conf.getLong(Constants.TEZ_RUNTIME_TASK_MEMORY,
        Math.min(Runtime.getRuntime().maxMemory(), Integer.MAX_VALUE)) * maxInMemCopyUse);

    final float singleShuffleMemoryLimitPercent = conf.getFloat(
        TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
        TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT);
    if (singleShuffleMemoryLimitPercent <= 0.0f
        || singleShuffleMemoryLimitPercent > 1.0f) {
      throw new IllegalArgumentException("Invalid value for "
          + TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT + ": "
          + singleShuffleMemoryLimitPercent);
    }

    this.maxSingleShuffleLimit = (long) (memoryLimit * singleShuffleMemoryLimitPercent);
    
    LOG.info("BroadcastInputManager -> " + "MemoryLimit: " + 
    this.memoryLimit + ", maxSingleMemLimit: " + this.maxSingleShuffleLimit);
  }

  @Override
  public synchronized FetchedInput allocate(long size,
      InputAttemptIdentifier inputAttemptIdentifier) throws IOException {
    if (size > maxSingleShuffleLimit
        || this.usedMemory + size > this.memoryLimit) {
      return new DiskFetchedInput(size, inputAttemptIdentifier, this, conf,
          localDirAllocator, fileNameAllocator);
    } else {
      this.usedMemory += size;
      LOG.info("Used memory after allocating " + size  + " : " + usedMemory);
      return new MemoryFetchedInput(size, inputAttemptIdentifier, this);
    }
  }

  @Override
  public synchronized void fetchComplete(FetchedInput fetchedInput) {
    switch (fetchedInput.getType()) {
    // Not tracking anything here.
    case DISK:
    case MEMORY:
      break;
    default:
      throw new TezUncheckedException("InputType: " + fetchedInput.getType()
          + " not expected for Broadcast fetch");
    }
  }

  @Override
  public synchronized void fetchFailed(FetchedInput fetchedInput) {
    cleanup(fetchedInput);
  }

  @Override
  public synchronized void freeResources(FetchedInput fetchedInput) {
    cleanup(fetchedInput);
  }

  private void cleanup(FetchedInput fetchedInput) {
    switch (fetchedInput.getType()) {
    case DISK:
      break;
    case MEMORY:
      unreserve(fetchedInput.getSize());
      break;
    default:
      throw new TezUncheckedException("InputType: " + fetchedInput.getType()
          + " not expected for Broadcast fetch");
    }
  }

  private synchronized void unreserve(long size) {
    this.usedMemory -= size;
    LOG.info("Used memory after freeing " + size  + " : " + usedMemory);
  }

}
