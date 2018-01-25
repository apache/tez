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

package org.apache.tez.test;

import org.apache.tez.dag.app.MockClock;
import org.apache.tez.dag.app.MockClock.MockClockListener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** A scheduled executor service with timing that can be controlled for unit tests. */
public class ControlledScheduledExecutorService implements ScheduledExecutorService, MockClockListener {
  private final MockClock clock;
  private final PriorityQueue<ScheduledFutureTask<?>> queue = new PriorityQueue<>();
  private final AtomicLong nextSequenceNum = new AtomicLong(0);
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  public ControlledScheduledExecutorService(MockClock clock) {
    this.clock = clock;
    clock.register(this);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    ScheduledFutureTask<Void> task = new ScheduledFutureTask<>(command, null, toTimestamp(delay, unit));
    schedule(task);
    return task;
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    ScheduledFutureTask<V> task = new ScheduledFutureTask<>(callable, toTimestamp(delay, unit));
    schedule(task);
    return task;
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    ScheduledFutureTask<Void> task = new ScheduledFutureTask<>(command, null,
        toTimestamp(initialDelay, unit), unit.toMillis(delay));
    schedule(task);
    return task;
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    return scheduleWithFixedDelay(command, initialDelay, period, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> callable) {
    ScheduledFutureTask<T> task = new ScheduledFutureTask<>(callable, 0);
    schedule(task);
    return task;
  }

  @Override
  public <T> Future<T> submit(Runnable runnable, T result) {
    ScheduledFutureTask<T> task = new ScheduledFutureTask<>(runnable, result, 0);
    schedule(task);
    return task;
  }

  @Override
  public Future<?> submit(Runnable runnable) {
    ScheduledFutureTask<?> task = new ScheduledFutureTask<>(runnable, null, 0);
    schedule(task);
    return task;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
    throw new UnsupportedOperationException("invokeAll not yet implemented");
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException("invokeAll not yet implemented");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException("invokeAny not yet implemented");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    throw new UnsupportedOperationException("invokeAny not yet implemented");
  }

  @Override
  public void execute(Runnable command) {
    submit(command);
  }

  @Override
  public void shutdown() {
    stopped.set(true);
  }

  @Override
  public List<Runnable> shutdownNow() {
    stopped.set(true);
    return new ArrayList<Runnable>(queue);
  }

  @Override
  public boolean isShutdown() {
    return stopped.get();
  }

  @Override
  public boolean isTerminated() {
    return false;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return false;
  }

  @Override
  public void onTimeUpdated(long newTime) {
     ScheduledFutureTask<?> task = queue.peek();
     while (task != null && task.timestamp <= newTime) {
       task = queue.poll();
       runTask(task);
       task = queue.peek();
     }
  }

  private long now() {
    return clock.getTime();
  }

  private long toTimestamp(long delay, TimeUnit unit) {
    return now() + unit.toMillis(delay);
  }

  private void schedule(ScheduledFutureTask<?> task) {
    if (isShutdown()) {
      throw new RejectedExecutionException("Executor has been shutdown");
    }
    if (now() - task.timestamp >= 0) {
      runTask(task);
    } else {
      queue.add(task);
    }
  }

  private void runTask(ScheduledFutureTask<?> task) {
    task.run();
    if (task.isPeriodic() && !isShutdown()) {
      task.timestamp = toTimestamp(task.period, TimeUnit.MILLISECONDS);
      queue.add(task);
    }
  }

  private class ScheduledFutureTask<V> extends FutureTask<V> implements RunnableScheduledFuture<V> {
    private final long sequenceNum;
    private final long period;
    private long timestamp;

    public ScheduledFutureTask(Callable<V> callable, long timestamp) {
      super(callable);
      this.sequenceNum = nextSequenceNum.getAndIncrement();
      this.timestamp = timestamp;
      this.period = 0;
    }

    public ScheduledFutureTask(Runnable runnable, V result, long timestamp) {
      super(runnable, result);
      this.sequenceNum = nextSequenceNum.getAndIncrement();
      this.timestamp = timestamp;
      this.period = 0;
    }

    public ScheduledFutureTask(Runnable runnable, V result, long timestamp, long period) {
      super(runnable, result);
      this.sequenceNum = nextSequenceNum.getAndIncrement();
      this.timestamp = timestamp;
      this.period = period;
    }

    @Override
    public boolean isPeriodic() {
      return period != 0;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(timestamp - now(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
      if (o == this) {
        return 0;
      }
      int result = Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
      if (result == 0 && o instanceof ScheduledFutureTask) {
        ScheduledFutureTask<?> otherTask = (ScheduledFutureTask<?>) o;
        result = Long.compare(sequenceNum, otherTask.sequenceNum);
      }
      return result;
    }
  }
}
