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

package org.apache.tez.runtime.library.shuffle.common.impl;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
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

import com.google.common.base.Preconditions;

/**
 * Usage: Create instance, setInitialMemoryAvailable(long), configureAndStart()
 *
 */
@Private
public class SimpleFetchedInputAllocator implements FetchedInputAllocator,
    FetchedInputCallback {

  private static final Log LOG = LogFactory.getLog(SimpleFetchedInputAllocator.class);
  
  private final Configuration conf;
  private final String uniqueIdentifier;

  private TezTaskOutputFiles fileNameAllocator;
  private LocalDirAllocator localDirAllocator;

  // Configuration parameters
  private long memoryLimit;
  private long maxSingleShuffleLimit;

  private volatile long usedMemory = 0;
  
  private long maxAvailableTaskMemory;
  private long initialMemoryAvailable =-1l;

  public SimpleFetchedInputAllocator(String uniqueIdentifier, Configuration conf, long maxTaskAvailableMemory) {
    this.conf = conf;    
    this.uniqueIdentifier = uniqueIdentifier;
    this.maxAvailableTaskMemory = maxTaskAvailableMemory;
  }

  @Private
  public void configureAndStart() {
    Preconditions.checkState(initialMemoryAvailable != -1,
        "Initial memory must be configured before starting");
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
    long memReq = (long) (conf.getLong(Constants.TEZ_RUNTIME_TASK_MEMORY,
        Math.min(maxAvailableTaskMemory, Integer.MAX_VALUE)) * maxInMemCopyUse);
    
    if (memReq <= this.initialMemoryAvailable) {
      this.memoryLimit = memReq;
    } else {
      this.memoryLimit = initialMemoryAvailable;
    }

    LOG.info("RequestedMem=" + memReq + ", Allocated: " + this.memoryLimit);

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
  
  @Private
  public static long getInitialMemoryReq(Configuration conf, long maxAvailableTaskMemory) {
    final float maxInMemCopyUse = conf.getFloat(
        TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT,
        TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for "
          + TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT + ": "
          + maxInMemCopyUse);
    }
    long memReq = (long) (conf.getLong(Constants.TEZ_RUNTIME_TASK_MEMORY,
        Math.min(maxAvailableTaskMemory, Integer.MAX_VALUE)) * maxInMemCopyUse);
    return memReq;
  }

  @Private
  public void setInitialMemoryAvailable(long available) {
    this.initialMemoryAvailable = available;
  }

  @Override
  public synchronized FetchedInput allocate(long actualSize, long compressedSize,
      InputAttemptIdentifier inputAttemptIdentifier) throws IOException {
    if (actualSize > maxSingleShuffleLimit
        || this.usedMemory + actualSize > this.memoryLimit) {
      return new DiskFetchedInput(actualSize, compressedSize,
          inputAttemptIdentifier, this, conf, localDirAllocator,
          fileNameAllocator);
    } else {
      this.usedMemory += actualSize;
      LOG.info("Used memory after allocating " + actualSize  + " : " + usedMemory);
      return new MemoryFetchedInput(actualSize, compressedSize, inputAttemptIdentifier, this);
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
      unreserve(fetchedInput.getActualSize());
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
