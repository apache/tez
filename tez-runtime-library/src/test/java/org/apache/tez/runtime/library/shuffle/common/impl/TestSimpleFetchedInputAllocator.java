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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.shuffle.common.FetchedInput;
import org.apache.tez.runtime.library.shuffle.common.impl.SimpleFetchedInputAllocator;
import org.junit.Test;

public class TestSimpleFetchedInputAllocator {

  private static final Log LOG = LogFactory.getLog(TestSimpleFetchedInputAllocator.class);
  
  @Test
  public void testInMemAllocation() throws IOException {
    String localDirs = "/tmp/" + this.getClass().getName();
    Configuration conf = new Configuration();
    
    long jvmMax = Runtime.getRuntime().maxMemory();
    LOG.info("jvmMax: " + jvmMax);
    
    float bufferPercent = 0.1f;
    conf.setFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT, bufferPercent);
    conf.setFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 1.0f);
    conf.setStrings(TezJobConfig.LOCAL_DIRS, localDirs);
    
    long inMemThreshold = (long) (bufferPercent * jvmMax);
    LOG.info("InMemThreshold: " + inMemThreshold);

    SimpleFetchedInputAllocator inputManager = new SimpleFetchedInputAllocator(UUID.randomUUID().toString(),
        conf, Runtime.getRuntime().maxMemory());
    inputManager.setInitialMemoryAvailable(inMemThreshold);
    inputManager.configureAndStart();

    long requestSize = (long) (0.4f * inMemThreshold);
    long compressedSize = 1l;
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
    
    // Freed one disk allocation. Next sould be disk again (no mem freed)
    fi3.abort();
    fi3.free();
    FetchedInput fi5 = inputManager.allocate(requestSize, compressedSize, new InputAttemptIdentifier(4, 1));
    assertEquals(FetchedInput.Type.DISK, fi5.getType());
  }

}
