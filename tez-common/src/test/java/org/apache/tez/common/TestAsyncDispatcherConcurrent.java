/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.common;

import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class TestAsyncDispatcherConcurrent {

  static class CountDownEventHandler {
    static CountDownLatch latch;
    static void init(CountDownLatch latch) {
      CountDownEventHandler.latch = latch;
    }

    static void checkParallelCountersDoneAndFinish() throws Exception {
      latch.countDown();
      latch.await();
    }
    
    public void handle() {
      latch.countDown();
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public enum TestEventType1 { TYPE1 }
  public class TestEvent1 extends TezAbstractEvent<TestEventType1> {
    final int hash;
    public TestEvent1(TestEventType1 type, int hash) {
      super(type);
      this.hash = hash;
    }
    
    @Override
    public int getSerializingHash() {
      return hash;
    }
  }
  class TestEventHandler1 extends CountDownEventHandler implements EventHandler<TestEvent1> {
    @Override
    public void handle(TestEvent1 event) {
      handle();
    }
  }
  public enum TestEventType2 { TYPE2 }
  public class TestEvent2 extends TezAbstractEvent<TestEventType2> {
    public TestEvent2(TestEventType2 type) {
      super(type);
    }
  }
  class TestEventHandler2 extends CountDownEventHandler implements EventHandler<TestEvent2> {
    @Override
    public void handle(TestEvent2 event) {
      handle();
    }
  }
  public enum TestEventType3 { TYPE3 }
  public class TestEvent3 extends TezAbstractEvent<TestEventType3> {
    public TestEvent3(TestEventType3 type) {
      super(type);
    }
  }
  class TestEventHandler3 extends CountDownEventHandler implements EventHandler<TestEvent3> {
    @Override
    public void handle(TestEvent3 event) {
      handle();
    }
  }

  @Test (timeout=5000)
  public void testBasic() throws Exception {
    CountDownLatch latch = new CountDownLatch(4);
    CountDownEventHandler.init(latch);
    
    AsyncDispatcher central = new AsyncDispatcher("Type1");
    central.register(TestEventType1.class, new TestEventHandler1());
    central.registerAndCreateDispatcher(TestEventType2.class, new TestEventHandler2(), "Type2", 1);
    central.registerAndCreateDispatcher(TestEventType3.class, new TestEventHandler3(), "Type3", 1);
    
    central.init(new Configuration());
    central.start();
    // 3 threads in different dispatchers will handle 3 events
    central.getEventHandler().handle(new TestEvent1(TestEventType1.TYPE1, 0));
    central.getEventHandler().handle(new TestEvent2(TestEventType2.TYPE2));
    central.getEventHandler().handle(new TestEvent3(TestEventType3.TYPE3));
    // wait for all events to be run in parallel
    CountDownEventHandler.checkParallelCountersDoneAndFinish();
    central.close();
  }
  
  @Test (timeout=5000)
  public void testMultiThreads() throws Exception {
    CountDownLatch latch = new CountDownLatch(4);
    CountDownEventHandler.init(latch);
    
    AsyncDispatcherConcurrent central = new AsyncDispatcherConcurrent("Type1", 1);
    central.registerAndCreateDispatcher(TestEventType1.class, new TestEventHandler1(), "Type1", 3);
    
    central.init(new Configuration());
    central.start();
    // 3 threads in the same dispatcher will handle 3 events
    central.getEventHandler().handle(new TestEvent1(TestEventType1.TYPE1, 0));
    central.getEventHandler().handle(new TestEvent1(TestEventType1.TYPE1, 1));
    central.getEventHandler().handle(new TestEvent1(TestEventType1.TYPE1, 2));
    // wait for all events to be run in parallel
    CountDownEventHandler.checkParallelCountersDoneAndFinish();
    central.close();
  }
  
  @Test (timeout=5000)
  public void testMultipleRegisterFail() throws Exception {
    AsyncDispatcher central = new AsyncDispatcher("Type1");
    try {
      central.register(TestEventType1.class, new TestEventHandler1());
      central.registerAndCreateDispatcher(TestEventType1.class, new TestEventHandler2(), "Type2", 1);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Cannot register same event on multiple dispatchers"));
    } finally {
      central.close();
    }
    
    central = new AsyncDispatcher("Type1");
    try {
      central.registerAndCreateDispatcher(TestEventType1.class, new TestEventHandler2(), "Type2", 1);
      central.register(TestEventType1.class, new TestEventHandler1());
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Multiple concurrent dispatchers cannot be registered"));
    } finally {
      central.close();
    }
    
    central = new AsyncDispatcher("Type1");
    try {
      central.registerAndCreateDispatcher(TestEventType1.class, new TestEventHandler2(), "Type2", 1);
      central.registerAndCreateDispatcher(TestEventType1.class, new TestEventHandler2(), "Type2", 1);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Multiple concurrent dispatchers cannot be registered"));
    } finally {
      central.close();
    }
    
    central = new AsyncDispatcher("Type1");
    try {
      central.registerAndCreateDispatcher(TestEventType1.class, new TestEventHandler2(), "Type2");
      central.registerAndCreateDispatcher(TestEventType1.class, new TestEventHandler2(), "Type2");
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Multiple dispatchers cannot be registered for"));
    } finally {
      central.close();
    }
    
    central = new AsyncDispatcher("Type1");
    try {
      AsyncDispatcherConcurrent concDispatcher = central.registerAndCreateDispatcher(
          TestEventType1.class, new TestEventHandler2(), "Type2", 1);
      central.registerWithExistingDispatcher(TestEventType1.class, new TestEventHandler1(),
          concDispatcher);
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Multiple concurrent dispatchers cannot be registered"));
    } finally {
      central.close();
    }
  }
}
