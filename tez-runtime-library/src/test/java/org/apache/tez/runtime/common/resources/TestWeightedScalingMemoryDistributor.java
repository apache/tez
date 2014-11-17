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

package org.apache.tez.runtime.common.resources;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.library.input.OrderedGroupedKVInput;
import org.apache.tez.runtime.library.input.UnorderedKVInput;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;
import org.apache.tez.runtime.library.resources.WeightedScalingMemoryDistributor;
import org.junit.Test;

import com.google.common.base.Joiner;

public class TestWeightedScalingMemoryDistributor extends TestMemoryDistributor {
  
  @Override
  public void setup() {
    conf.setBoolean(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ENABLED, true);
    conf.set(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ALLOCATOR_CLASS,
        WeightedScalingMemoryDistributor.class.getName());
    conf.setDouble(TezConfiguration.TEZ_TASK_SCALE_MEMORY_RESERVE_FRACTION, 0.3d);
    conf.setDouble(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_PER_IO, 0.0d);
  }
  
  @Test(timeout = 5000)
  public void testSimpleWeightedScaling() {
    Configuration conf = new Configuration(this.conf);
    conf.setStrings(TezConfiguration.TEZ_TASK_SCALE_MEMORY_WEIGHTED_RATIOS,
        WeightedScalingMemoryDistributor.generateWeightStrings(0, 1, 2, 3, 1, 1));
    System.err.println(Joiner.on(",").join(conf.getStringCollection(
        TezConfiguration.TEZ_TASK_SCALE_MEMORY_WEIGHTED_RATIOS)));

    MemoryDistributor dist = new MemoryDistributor(2, 2, conf);

    dist.setJvmMemory(10000l);

    // First request - ScatterGatherShuffleInput
    MemoryUpdateCallbackForTest e1Callback = new MemoryUpdateCallbackForTest();
    InputContext e1InputContext1 = createTestInputContext();
    InputDescriptor e1InDesc1 = createTestInputDescriptor(OrderedGroupedKVInput.class);
    dist.requestMemory(10000, e1Callback, e1InputContext1, e1InDesc1);

    // Second request - BroadcastInput
    MemoryUpdateCallbackForTest e2Callback = new MemoryUpdateCallbackForTest();
    InputContext e2InputContext2 = createTestInputContext();
    InputDescriptor e2InDesc2 = createTestInputDescriptor(UnorderedKVInput.class);
    dist.requestMemory(10000, e2Callback, e2InputContext2, e2InDesc2);

    // Third request - randomOutput (simulates MROutput)
    MemoryUpdateCallbackForTest e3Callback = new MemoryUpdateCallbackForTest();
    OutputContext e3OutputContext1 = createTestOutputContext();
    OutputDescriptor e3OutDesc1 = createTestOutputDescriptor();
    dist.requestMemory(10000, e3Callback, e3OutputContext1, e3OutDesc1);

    // Fourth request - OnFileSortedOutput
    MemoryUpdateCallbackForTest e4Callback = new MemoryUpdateCallbackForTest();
    OutputContext e4OutputContext2 = createTestOutputContext();
    OutputDescriptor e4OutDesc2 = createTestOutputDescriptor(OrderedPartitionedKVOutput.class);
    dist.requestMemory(10000, e4Callback, e4OutputContext2, e4OutDesc2);

    dist.makeInitialAllocations();

    // Total available: 70% of 10K = 7000
    // 4 requests (weight) - 10K (3), 10K(1), 10K(1), 10K(2)
    // Scale down to - 3000, 1000, 1000, 2000
    assertEquals(3000, e1Callback.assigned);
    assertEquals(1000, e2Callback.assigned);
    assertEquals(1000, e3Callback.assigned);
    assertEquals(2000, e4Callback.assigned);
  }

  @Test(timeout = 5000)
  public void testAdditionalReserveFractionWeightedScaling() {
    Configuration conf = new Configuration(this.conf);
    conf.setStrings(TezConfiguration.TEZ_TASK_SCALE_MEMORY_WEIGHTED_RATIOS,
        WeightedScalingMemoryDistributor.generateWeightStrings(0, 2, 3, 6, 1, 1));
    conf.setDouble(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_PER_IO, 0.025d);
    conf.setDouble(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ADDITIONAL_RESERVATION_FRACTION_MAX, 0.2d);

    MemoryDistributor dist = new MemoryDistributor(2, 2, conf);

    dist.setJvmMemory(10000l);

    // First request - ScatterGatherShuffleInput [weight 6]
    MemoryUpdateCallbackForTest e1Callback = new MemoryUpdateCallbackForTest();
    InputContext e1InputContext1 = createTestInputContext();
    InputDescriptor e1InDesc1 = createTestInputDescriptor(OrderedGroupedKVInput.class);
    dist.requestMemory(10000, e1Callback, e1InputContext1, e1InDesc1);

    // Second request - BroadcastInput [weight 2]
    MemoryUpdateCallbackForTest e2Callback = new MemoryUpdateCallbackForTest();
    InputContext e2InputContext2 = createTestInputContext();
    InputDescriptor e2InDesc2 = createTestInputDescriptor(UnorderedKVInput.class);
    dist.requestMemory(10000, e2Callback, e2InputContext2, e2InDesc2);

    // Third request - randomOutput (simulates MROutput) [weight 1]
    MemoryUpdateCallbackForTest e3Callback = new MemoryUpdateCallbackForTest();
    OutputContext e3OutputContext1 = createTestOutputContext();
    OutputDescriptor e3OutDesc1 = createTestOutputDescriptor();
    dist.requestMemory(10000, e3Callback, e3OutputContext1, e3OutDesc1);

    // Fourth request - OnFileSortedOutput [weight 3]
    MemoryUpdateCallbackForTest e4Callback = new MemoryUpdateCallbackForTest();
    OutputContext e4OutputContext2 = createTestOutputContext();
    OutputDescriptor e4OutDesc2 = createTestOutputDescriptor(OrderedPartitionedKVOutput.class);
    dist.requestMemory(10000, e4Callback, e4OutputContext2, e4OutDesc2);

    dist.makeInitialAllocations();

    // Total available: 60% of 10K = 7000
    // 4 requests (weight) - 10K (6), 10K(2), 10K(1), 10K(3)
    // Scale down to - 3000, 1000, 500, 1500
    assertEquals(3000, e1Callback.assigned);
    assertEquals(1000, e2Callback.assigned);
    assertEquals(500, e3Callback.assigned);
    assertEquals(1500, e4Callback.assigned);
  }
  
  private static class MemoryUpdateCallbackForTest extends MemoryUpdateCallback {

    long assigned = -1000;

    @Override
    public void memoryAssigned(long assignedSize) {
      this.assigned = assignedSize;
    }
  }

  private InputDescriptor createTestInputDescriptor(Class<? extends LogicalInput> inputClazz) {
    InputDescriptor desc = mock(InputDescriptor.class);
    doReturn(inputClazz.getName()).when(desc).getClassName();
    return desc;
  }

  private OutputDescriptor createTestOutputDescriptor(Class<? extends LogicalOutput> outputClazz) {
    OutputDescriptor desc = mock(OutputDescriptor.class);
    doReturn(outputClazz.getName()).when(desc).getClassName();
    return desc;
  }

}
