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
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.junit.Assert;
import org.junit.Test;

public class TestAsyncDispatcher {

  static class CountDownEventHandler {
    static CountDownLatch latch;
    public void handle() {
      latch.countDown();
      try {
        latch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public enum TestEventType1 { TYPE1 }
  public class TestEvent1 extends AbstractEvent<TestEventType1> {
    public TestEvent1(TestEventType1 type) {
      super(type);
    }
  }
  class TestEventHandler1 extends CountDownEventHandler implements EventHandler<TestEvent1> {
    @Override
    public void handle(TestEvent1 event) {
      handle();
    }
  }
  public enum TestEventType2 { TYPE2 }
  public class TestEvent2 extends AbstractEvent<TestEventType2> {
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
  public class TestEvent3 extends AbstractEvent<TestEventType3> {
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
  
  @SuppressWarnings("unchecked")
  @Test (timeout=5000)
  public void testBasic() throws Exception {
    CountDownLatch latch = new CountDownLatch(4);
    CountDownEventHandler.latch = latch;
    
    AsyncDispatcher central = new AsyncDispatcher("Type1");
    central.register(TestEventType1.class, new TestEventHandler1());
    central.registerAndCreateDispatcher(TestEventType2.class, new TestEventHandler2(), "Type2");
    central.registerAndCreateDispatcher(TestEventType3.class, new TestEventHandler3(), "Type3");
    
    central.init(new Configuration());
    central.start();
    central.getEventHandler().handle(new TestEvent1(TestEventType1.TYPE1));
    central.getEventHandler().handle(new TestEvent2(TestEventType2.TYPE2));
    central.getEventHandler().handle(new TestEvent3(TestEventType3.TYPE3));
    latch.countDown();
    latch.await();
    central.close();
  }
  
  @Test (timeout=5000)
  public void testMultipleRegisterFail() throws Exception {
    AsyncDispatcher central = new AsyncDispatcher("Type1");
    try {
      central.register(TestEventType1.class, new TestEventHandler1());
      central.registerAndCreateDispatcher(TestEventType1.class, new TestEventHandler2(), "Type2");
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Cannot register same event on multiple dispatchers"));
    } finally {
      central.close();
    }
    
    central = new AsyncDispatcher("Type1");
    try {
      central.registerAndCreateDispatcher(TestEventType1.class, new TestEventHandler2(), "Type2");
      central.register(TestEventType1.class, new TestEventHandler1());
      Assert.fail();
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().contains("Multiple dispatchers cannot be registered for"));
    } finally {
      central.close();
    }
  }
}
