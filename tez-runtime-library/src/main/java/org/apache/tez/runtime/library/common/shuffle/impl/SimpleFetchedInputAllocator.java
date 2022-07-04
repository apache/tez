/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.apache.tez.runtime.library.common.shuffle.DiskFetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput.Type;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputCallback;
import org.apache.tez.runtime.library.common.shuffle.MemoryFetchedInput;

/**
 * Usage: Create instance, setInitialMemoryAvailable(long), configureAndStart()
 *
 */
@Private
public class SimpleFetchedInputAllocator implements FetchedInputAllocator,
    FetchedInputCallback {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleFetchedInputAllocator.class);

  private final Configuration conf;

  private final TezTaskOutputFiles fileNameAllocator;
  private final LocalDirAllocator localDirAllocator;

  // Configuration parameters
  private final long memoryLimit;
  private final long maxSingleShuffleLimit;

  private final long maxAvailableTaskMemory;
  private final long initialMemoryAvailable;

  private final String srcNameTrimmed;

  private volatile long usedMemory = 0;

  public SimpleFetchedInputAllocator(String srcNameTrimmed,
                                     String uniqueIdentifier, int dagID,
                                     Configuration conf,
                                     long maxTaskAvailableMemory,
                                     long memoryAvailable) {
    this.srcNameTrimmed = srcNameTrimmed;
    this.conf = conf;
    this.maxAvailableTaskMemory = maxTaskAvailableMemory;
    this.initialMemoryAvailable = memoryAvailable;

    this.fileNameAllocator = new TezTaskOutputFiles(conf,
        uniqueIdentifier, dagID);
    this.localDirAllocator = new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);

    // Setup configuration
    final float maxInMemCopyUse = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for "
          + TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT + ": "
          + maxInMemCopyUse);
    }

    long memReq = (long) (conf.getLong(Constants.TEZ_RUNTIME_TASK_MEMORY,
        Math.min(maxAvailableTaskMemory, Integer.MAX_VALUE)) * maxInMemCopyUse);

    if (memReq <= this.initialMemoryAvailable) {
      this.memoryLimit = memReq;
    } else {
      this.memoryLimit = initialMemoryAvailable;
    }

    final float singleShuffleMemoryLimitPercent = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT_DEFAULT);
    if (singleShuffleMemoryLimitPercent <= 0.0f
        || singleShuffleMemoryLimitPercent > 1.0f) {
      throw new IllegalArgumentException("Invalid value for "
          + TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT + ": "
          + singleShuffleMemoryLimitPercent);
    }

    //TODO: cap it to MAX_VALUE until MemoryFetchedInput can handle > 2 GB
    this.maxSingleShuffleLimit = (long) Math.min((memoryLimit * singleShuffleMemoryLimitPercent),
        Integer.MAX_VALUE);

    LOG.info(srcNameTrimmed + ": "
        + "RequestedMemory=" + memReq
        + ", AssignedMemory=" + this.memoryLimit
        + ", maxSingleShuffleLimit=" + this.maxSingleShuffleLimit
    );
  }

  @Private
  public static long getInitialMemoryReq(Configuration conf, long maxAvailableTaskMemory) {
    final float maxInMemCopyUse = conf.getFloat(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT_DEFAULT);
    if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
      throw new IllegalArgumentException("Invalid value for "
          + TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT + ": "
          + maxInMemCopyUse);
    }
    long memReq = (long) (conf.getLong(Constants.TEZ_RUNTIME_TASK_MEMORY,
        Math.min(maxAvailableTaskMemory, Integer.MAX_VALUE)) * maxInMemCopyUse);
    return memReq;
  }

  @Override
  public synchronized FetchedInput allocate(long actualSize, long compressedSize,
                                            InputAttemptIdentifier inputAttemptIdentifier) throws IOException {
    if (actualSize > maxSingleShuffleLimit
        || this.usedMemory + actualSize > this.memoryLimit) {
      return new DiskFetchedInput(compressedSize,
          inputAttemptIdentifier, this, conf, localDirAllocator,
          fileNameAllocator);
    } else {
      this.usedMemory += actualSize;
      if (LOG.isDebugEnabled()) {
        LOG.info(srcNameTrimmed + ": " + "Used memory after allocating " + actualSize + " : " +
            usedMemory);
      }
      return new MemoryFetchedInput(actualSize, inputAttemptIdentifier, this);
    }
  }

  @Override
  public synchronized FetchedInput allocateType(Type type, long actualSize,
                                                long compressedSize, InputAttemptIdentifier inputAttemptIdentifier)
      throws IOException {

    switch (type) {
      case DISK:
        return new DiskFetchedInput(compressedSize,
            inputAttemptIdentifier, this, conf, localDirAllocator,
            fileNameAllocator);
      default:
        return allocate(actualSize, compressedSize, inputAttemptIdentifier);
    }
  }

  @Override
  public synchronized void fetchComplete(FetchedInput fetchedInput) {
    switch (fetchedInput.getType()) {
      // Not tracking anything here.
      case DISK:
      case DISK_DIRECT:
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
        unreserve(((MemoryFetchedInput) fetchedInput).getSize());
        break;
      default:
        throw new TezUncheckedException("InputType: " + fetchedInput.getType()
            + " not expected for Broadcast fetch");
    }
  }

  private synchronized void unreserve(long size) {
    this.usedMemory -= size;
    if (LOG.isDebugEnabled()) {
      LOG.debug(srcNameTrimmed + ": " + "Used memory after freeing " + size + " : " + usedMemory);
    }
  }
}
