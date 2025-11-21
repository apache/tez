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

package org.apache.tez.runtime.library.common.shuffle.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSimpleFetchedInputAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(TestSimpleFetchedInputAllocator.class);

  @Test(timeout = 5000)
  public void testInMemAllocation() throws IOException {
    File localDirs = new File(System.getProperty("test.build.data", "/tmp"), this.getClass().getName());
    Configuration conf = new Configuration();

    long jvmMax = 954728448L;
    LOG.info("jvmMax: " + jvmMax);

    float bufferPercent = 0.1f;
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, bufferPercent);
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 1.0f);
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs.getAbsolutePath());

    long inMemThreshold = (long) (bufferPercent * jvmMax);
    LOG.info("InMemThreshold: " + inMemThreshold);

    SimpleFetchedInputAllocator inputManager = new SimpleFetchedInputAllocator(
            "srcName", UUID.randomUUID().toString(), 123, conf,
            jvmMax, inMemThreshold);

    long requestSize = (long) (0.4f * inMemThreshold);
    long compressedSize = 1L;
    LOG.info("RequestSize: " + requestSize);

    FetchedInput fi1 = inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(1, 1));
    assertEquals(FetchedInput.Type.MEMORY, fi1.getType());

    FetchedInput fi2 = inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(2, 1));
    assertEquals(FetchedInput.Type.MEMORY, fi2.getType());

    // Over limit by this point. Next reserve should give back a DISK allocation
    FetchedInput fi3 = inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(3, 1));
    assertEquals(FetchedInput.Type.DISK, fi3.getType());

    // Freed one memory allocation. Next should be mem again.
    fi1.abort();
    fi1.free();
    FetchedInput fi4 = inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(4, 1));
    assertEquals(FetchedInput.Type.MEMORY, fi4.getType());

    // Freed one disk allocation. Next should be disk again (no mem freed)
    fi3.abort();
    fi3.free();
    FetchedInput fi5 = inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(4, 1));
    assertEquals(FetchedInput.Type.DISK, fi5.getType());
  }

  /**
   * This method tests the allocation behavior of SimpleFetchedInputAllocator when
   * a high `maxMemory` is reported by the Runtime.The allocation results in a
   * DISK input because the `requestSize` exceeds the `maxSingleShuffleLimit`.
   */
  @Test(timeout = 5000)
  public void testInMemAllocationWithJvmMaxMemory() throws IOException {
    File localDirs = new File(System.getProperty("test.build.data", "/tmp"), this.getClass().getName());
    Configuration conf = new Configuration();

    long jvmMax = Runtime.getRuntime().maxMemory();
    LOG.info("jvmMax: " + jvmMax);

    float bufferPercent = 0.1f;
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_BUFFER_PERCENT, bufferPercent);
    conf.setFloat(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 1.0f);
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs.getAbsolutePath());

    long inMemThreshold = (long) (bufferPercent * jvmMax);
    LOG.info("InMemThreshold: " + inMemThreshold);

    SimpleFetchedInputAllocator inputManager = new SimpleFetchedInputAllocator(
            "srcName", UUID.randomUUID().toString(), 123, conf,
            jvmMax, inMemThreshold);

    long requestSize = (long) (0.4f * inMemThreshold) + 100L;
    long compressedSize = 1L;
    LOG.info("RequestSize: " + requestSize);

    // check if requestSize is greater than maxSingleShuffleLimit
    assertTrue(requestSize > inputManager.maxSingleShuffleLimit);

    // check if really test case failure catch in tez ci
    assertTrue(requestSize < inputManager.maxSingleShuffleLimit);

    // requestSize is greater than the maxSingleShuffleLimit, so allocation is from DISK
    FetchedInput fi1 = inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(1, 1));
    assertEquals(FetchedInput.Type.DISK, fi1.getType());
  }
}
