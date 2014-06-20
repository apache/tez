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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.junit.Before;
import org.junit.Test;

public class TestMemoryDistributor {

  protected Configuration conf = new Configuration();
  
  @Before
  public void setup() {
    conf.setBoolean(TezJobConfig.TEZ_RUNTIME_SCALE_TASK_MEMORY_ENABLED, true);
    conf.set(TezJobConfig.TEZ_RUNTIME_SCALE_TASK_MEMORY_ALLOCATOR_CLASS,
        ScalingAllocator.class.getName());
  }
  
  @Test(timeout = 5000)
  public void testScalingNoProcessor() {
    MemoryDistributor dist = new MemoryDistributor(2, 1, conf);
    
    dist.setJvmMemory(10000l);

    // First request
    MemoryUpdateCallbackForTest e1Callback = new MemoryUpdateCallbackForTest();
    TezInputContext e1InputContext1 = createTestInputContext();
    InputDescriptor e1InDesc1 = createTestInputDescriptor();
    dist.requestMemory(10000, e1Callback, e1InputContext1, e1InDesc1);
    
    // Second request
    MemoryUpdateCallbackForTest e2Callback = new MemoryUpdateCallbackForTest();
    TezInputContext e2InputContext2 = createTestInputContext();
    InputDescriptor e2InDesc2 = createTestInputDescriptor();
    dist.requestMemory(10000, e2Callback, e2InputContext2, e2InDesc2);
    
    // Third request - output
    MemoryUpdateCallbackForTest e3Callback = new MemoryUpdateCallbackForTest();
    TezOutputContext e3OutputContext1 = createTestOutputContext();
    OutputDescriptor e3OutDesc2 = createTestOutputDescriptor();
    dist.requestMemory(5000, e3Callback, e3OutputContext1, e3OutDesc2);
    
    dist.makeInitialAllocations();
    
    // Total available: 70% of 10K = 7000
    // 3 requests - 10K, 10K, 5K
    // Scale down to - 2800, 2800, 1400
    assertEquals(2800, e1Callback.assigned);
    assertEquals(2800, e2Callback.assigned);
    assertEquals(1400, e3Callback.assigned);
  }
  
  @Test(timeout = 5000)
  public void testScalingNoProcessor2() {
    // Real world values
    MemoryDistributor dist = new MemoryDistributor(2, 0, conf);
    
    dist.setJvmMemory(209715200l);

    // First request
    MemoryUpdateCallbackForTest e1Callback = new MemoryUpdateCallbackForTest();
    TezInputContext e1InputContext1 = createTestInputContext();
    InputDescriptor e1InDesc1 = createTestInputDescriptor();
    dist.requestMemory(104857600l, e1Callback, e1InputContext1, e1InDesc1);
    
    // Second request
    MemoryUpdateCallbackForTest e2Callback = new MemoryUpdateCallbackForTest();
    TezInputContext e2InputContext2 = createTestInputContext();
    InputDescriptor e2InDesc2 = createTestInputDescriptor();
    dist.requestMemory(157286400l, e2Callback, e2InputContext2, e2InDesc2);
    
    dist.makeInitialAllocations();

    assertEquals(58720256l, e1Callback.assigned);
    assertEquals(88080384l, e2Callback.assigned);
  }
  
  @Test(timeout = 5000)
  public void testScalingProcessor() {
    MemoryDistributor dist = new MemoryDistributor(2, 1, conf);
    
    dist.setJvmMemory(10000l);

    // First request
    MemoryUpdateCallbackForTest e1Callback = new MemoryUpdateCallbackForTest();
    TezInputContext e1InputContext1 = createTestInputContext();
    InputDescriptor e1InDesc1 = createTestInputDescriptor();
    dist.requestMemory(10000, e1Callback, e1InputContext1, e1InDesc1);
    
    // Second request
    MemoryUpdateCallbackForTest e2Callback = new MemoryUpdateCallbackForTest();
    TezInputContext e2InputContext2 = createTestInputContext();
    InputDescriptor e2InDesc2 = createTestInputDescriptor();
    dist.requestMemory(10000, e2Callback, e2InputContext2, e2InDesc2);
    
    // Third request - output
    MemoryUpdateCallbackForTest e3Callback = new MemoryUpdateCallbackForTest();
    TezOutputContext e3OutputContext1 = createTestOutputContext();
    OutputDescriptor e3OutDesc1 = createTestOutputDescriptor();
    dist.requestMemory(5000, e3Callback, e3OutputContext1, e3OutDesc1);
    
    // Fourth request - processor
    MemoryUpdateCallbackForTest e4Callback = new MemoryUpdateCallbackForTest();
    TezProcessorContext e4ProcessorContext1 = createTestProcessortContext();
    ProcessorDescriptor e4ProcessorDesc1 = createTestProcessorDescriptor();
    dist.requestMemory(5000, e4Callback, e4ProcessorContext1, e4ProcessorDesc1);
    
    
    dist.makeInitialAllocations();
    
    // Total available: 70% of 10K = 7000
    // 4 requests - 10K, 10K, 5K, 5K
    // Scale down to - 2333.33, 2333.33, 1166.66, 1166.66
    assertTrue(e1Callback.assigned >= 2333 && e1Callback.assigned <= 2334);
    assertTrue(e2Callback.assigned >= 2333 && e2Callback.assigned <= 2334);
    assertTrue(e3Callback.assigned >= 1166 && e3Callback.assigned <= 1167);
    assertTrue(e4Callback.assigned >= 1166 && e4Callback.assigned <= 1167);
  }
  
  @Test(timeout = 5000)
  public void testScalingDisabled() {
    // Real world values
    Configuration conf = new Configuration(this.conf);
    conf.setBoolean(TezJobConfig.TEZ_RUNTIME_SCALE_TASK_MEMORY_ENABLED, false);
    MemoryDistributor dist = new MemoryDistributor(2, 0, conf);
    
    dist.setJvmMemory(207093760l);

    // First request
    MemoryUpdateCallbackForTest e1Callback = new MemoryUpdateCallbackForTest();
    TezInputContext e1InputContext1 = createTestInputContext();
    InputDescriptor e1InDesc1 = createTestInputDescriptor();
    dist.requestMemory(104857600l, e1Callback, e1InputContext1, e1InDesc1);
    
    // Second request
    MemoryUpdateCallbackForTest e2Callback = new MemoryUpdateCallbackForTest();
    TezInputContext e2InputContext2 = createTestInputContext();
    InputDescriptor e2InDesc2 = createTestInputDescriptor();
    dist.requestMemory(144965632l, e2Callback, e2InputContext2, e2InDesc2);
    
    dist.makeInitialAllocations();

    assertEquals(104857600l, e1Callback.assigned);
    assertEquals(144965632l, e2Callback.assigned);
  }
  
  @Test(timeout = 5000)
  public void testReserveFractionConfigured() {
    Configuration conf = new Configuration(this.conf);
    conf.setDouble(TezJobConfig.TEZ_RUNTIME_SCALE_TASK_MEMORY_RESERVE_FRACTION, 0.5d);
    MemoryDistributor dist = new MemoryDistributor(2, 1, conf);
    
    dist.setJvmMemory(10000l);

    // First request
    MemoryUpdateCallbackForTest e1Callback = new MemoryUpdateCallbackForTest();
    TezInputContext e1InputContext1 = createTestInputContext();
    InputDescriptor e1InDesc1 = createTestInputDescriptor();
    dist.requestMemory(10000, e1Callback, e1InputContext1, e1InDesc1);
    
    // Second request
    MemoryUpdateCallbackForTest e2Callback = new MemoryUpdateCallbackForTest();
    TezInputContext e2InputContext2 = createTestInputContext();
    InputDescriptor e2InDesc2 = createTestInputDescriptor();
    dist.requestMemory(10000, e2Callback, e2InputContext2, e2InDesc2);
    
    // Third request - output
    MemoryUpdateCallbackForTest e3Callback = new MemoryUpdateCallbackForTest();
    TezOutputContext e3OutputContext1 = createTestOutputContext();
    OutputDescriptor e3OutDesc2 = createTestOutputDescriptor();
    dist.requestMemory(5000, e3Callback, e3OutputContext1, e3OutDesc2);
    
    dist.makeInitialAllocations();
    
    // Total available: 50% of 10K = 7000
    // 3 requests - 10K, 10K, 5K
    // Scale down to - 2000, 2000, 1000
    assertEquals(2000, e1Callback.assigned);
    assertEquals(2000, e2Callback.assigned);
    assertEquals(1000, e3Callback.assigned);
  }
  
  
  private static class MemoryUpdateCallbackForTest implements MemoryUpdateCallback {

    long assigned = -1000;

    @Override
    public void memoryAssigned(long assignedSize) {
      this.assigned = assignedSize;
    }
  }

  protected InputDescriptor createTestInputDescriptor() {
    InputDescriptor desc = mock(InputDescriptor.class);
    doReturn("InputClass").when(desc).getClassName();
    return desc;
  }

  protected OutputDescriptor createTestOutputDescriptor() {
    OutputDescriptor desc = mock(OutputDescriptor.class);
    doReturn("OutputClass").when(desc).getClassName();
    return desc;
  }

  protected ProcessorDescriptor createTestProcessorDescriptor() {
    ProcessorDescriptor desc = mock(ProcessorDescriptor.class);
    doReturn("ProcessorClass").when(desc).getClassName();
    return desc;
  }

  protected TezInputContext createTestInputContext() {
    TezInputContext context = mock(TezInputContext.class);
    doReturn("input").when(context).getSourceVertexName();
    doReturn("task").when(context).getTaskVertexName();
    return context;
  }
  
  protected TezOutputContext createTestOutputContext() {
    TezOutputContext context = mock(TezOutputContext.class);
    doReturn("output").when(context).getDestinationVertexName();
    doReturn("task").when(context).getTaskVertexName();
    return context;
  }
  
  protected TezProcessorContext createTestProcessortContext() {
    TezProcessorContext context = mock(TezProcessorContext.class);
    doReturn("task").when(context).getTaskVertexName();
    return context;
  }

}
