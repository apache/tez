/*
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

package org.apache.tez.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestTezSharedExecutor {

  private static class Sleep implements Runnable {
    private final long sleepTime;
    Sleep(long sleepTime) {
      this.sleepTime = sleepTime;
    }
    @Override
    public void run() {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static class Wait implements Runnable {
    private final CountDownLatch latch;
    Wait(CountDownLatch latch) {
      this.latch = latch;
    }
    @Override
    public void run() {
      try {
        latch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static class Counter implements Runnable {
    private final AtomicInteger counter;
    Counter(ConcurrentHashMap<String, AtomicInteger> map, String tag) {
      if (!map.contains(tag)) {
        map.putIfAbsent(tag, new AtomicInteger(0));
      }
      this.counter = map.get(tag);
    }
    @Override
    public void run() {
      counter.getAndIncrement();
    }
  }

  private static class Appender<T> implements Runnable {
    private final Collection<T> collection;
    private final T obj;
    Appender(Collection<T> collection, T obj) {
      this.collection = collection;
      this.obj = obj;
    }
    @Override
    public void run() {
      collection.add(obj);
    }
  }

  private static class Runner implements Runnable {
    private Runnable[] runnables;
    Runner(Runnable ... runnables) {
      this.runnables = runnables;
    }
    @Override
    public void run() {
      for (Runnable runnable : runnables) {
        runnable.run();
      }
    }
  }

  private TezSharedExecutor sharedExecutor;

  @Before
  public void setup() {
    sharedExecutor = new TezSharedExecutor(new Configuration());
  }

  @After
  public void cleanup() {
    sharedExecutor.shutdownNow();
    sharedExecutor = null;
  }

  @Test(timeout=10000)
  public void testSimpleExecution() throws Exception {
    ConcurrentHashMap<String, AtomicInteger> map = new ConcurrentHashMap<>();

    ExecutorService service = sharedExecutor.createExecutorService(1, "simple-test");

    // Test runnable
    service.submit(new Counter(map, "test")).get();
    Assert.assertEquals(1, map.get("test").get());

    // Test runnable with a result
    final Object expected = new Object();
    Object val = service.submit(new Counter(map, "test"), expected).get();
    Assert.assertEquals(expected, val);
    Assert.assertEquals(2, map.get("test").get());

    // Test callable.
    val = service.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return expected;
      }
    }).get();
    Assert.assertEquals(expected, val);

    // Tasks should be rejected after a shutdown.
    service.shutdown();

    try {
      service.submit(new Counter(map, "test"));
      Assert.fail("Expected rejected execution exception.");
    } catch (RejectedExecutionException e) {
    }
  }

  @Test(timeout=10000)
  public void testAwaitTermination() throws Exception {
    ExecutorService service = sharedExecutor.createExecutorService(1, "await-termination");
    CountDownLatch latch = new CountDownLatch(1);

    final Runnable runnable = new Wait(latch);
    service.submit(runnable);
    service.shutdown();

    // Task stuck on latch hence it should fail to terminate.
    Assert.assertFalse(service.awaitTermination(100, TimeUnit.MILLISECONDS));
    Assert.assertFalse(service.isTerminated());
    Assert.assertTrue(service.isShutdown());

    latch.countDown();

    Assert.assertTrue(service.awaitTermination(5, TimeUnit.SECONDS));
    Assert.assertTrue(service.isTerminated());
    Assert.assertTrue(service.isShutdown());
  }

  @Test(timeout=10000)
  public void testSerialExecution() throws Exception {
    ExecutorService service = sharedExecutor.createExecutorService(1, "serial-test");
    CountDownLatch latch = new CountDownLatch(1);

    // Since it is serial we should never get concurrent modification exception too.
    Future<?> f1 = service.submit(new Wait(latch));
    List<Integer> list = new ArrayList<>();
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      futures.add(service.submit(new Appender<Integer>(list, i)));
    }

    // This shutdown does not prevent already submitted tasks from completing.
    service.shutdown();

    // Until we release the task from the latch nothing moves forward.
    Assert.assertEquals(0, list.size());
    latch.countDown();
    f1.get();

    // Wait for all futures to finish.
    for (Future<?> f : futures) {
      f.get();
    }
    Assert.assertEquals(10, list.size());
    Assert.assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), list);
  }

  @Test(timeout=10000)
  public void testParallelExecution() throws Exception {
    ConcurrentHashMap<String, AtomicInteger> map = new ConcurrentHashMap<>();

    List<Future<?>> futures = new ArrayList<>();
    ExecutorService[] services = {
        sharedExecutor.createExecutorService(2, "parallel-1"),
        sharedExecutor.createExecutorService(2, "parallel-2")
    };
    int[] expectedCounts = {0, 0};
    Random random = new Random();
    for (int i = 0; i < 200; ++i) {
      int serviceIndex = random.nextInt(2);
      expectedCounts[serviceIndex] += 1;
      futures.add(services[serviceIndex].submit(
          new Runner(new Sleep(10), new Counter(map, "test" + serviceIndex))));
    }
    for (Future<?> future : futures) {
      future.get();
    }
    Assert.assertEquals(expectedCounts[0], map.get("test0").get());
    Assert.assertEquals(expectedCounts[1], map.get("test1").get());

    // Even if one service is shutdown the other should work.
    services[0].shutdown();
    services[1].submit(new Counter(map, "test1")).get();
    Assert.assertEquals(expectedCounts[1] + 1, map.get("test1").get());
  }

}
