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
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * An ExecutorService factory which shares threads between executors created using this service.
 */
@Private
@Unstable
public class TezSharedExecutor implements TezExecutors {

  // The shared executor service which will be used to execute all the tasks.
  private final ThreadPoolExecutor service;

  private final DelayedExecutionPoller poller;

  public TezSharedExecutor(Configuration conf) {
    // The default value is 0. We could start with a few threads so that thread pool is never empty.
    int minThreads = conf.getInt(TezConfiguration.TEZ_SHARED_EXECUTOR_MIN_THREADS,
        TezConfiguration.TEZ_SHARED_EXECUTOR_MIN_THREADS_DEFAULT);
    // The default value is Integer.MAX_VALUE, but ExecutorServiceInternal will do the rate limiting
    // of total numbers of tasks and hence the num threads will be bounded.
    int maxThreads = conf.getInt(TezConfiguration.TEZ_SHARED_EXECUTOR_MAX_THREADS,
        TezConfiguration.TEZ_SHARED_EXECUTOR_MAX_THREADS_DEFAULT);
    if (maxThreads < 0) {
      maxThreads = Integer.MAX_VALUE;
    }
    this.service = new ThreadPoolExecutor(
        minThreads, maxThreads,
        // The timeout is to give thread a chance to be re-used instead of being cleaned up.
        60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TezSharedExecutor: %d").build());

    // Setup polling thread to pick new tasks from the underlying executors.
    poller = new DelayedExecutionPoller(service);
    poller.start();
  }

  public ExecutorService createExecutorService(int poolSize, String threadName) {
    return new ExecutorServiceInternal(poolSize, threadName);
  }

  // Should we allow a shared service shutdown, once this shutdown is complete, all the executors
  // are in shutdown mode and will throw exception if we try to submit new tasks. And already
  // submitted tasks in the ExecutorServiceInternal which are not yet submitted to the shared
  // service will not be executed. That break contracts, we can fix this by tracking that the
  // service is shutdown and wait until all the dependent.
  public void shutdown() {
    service.shutdown();
    poller.interrupt();
  }

  public void shutdownNow() {
    service.shutdownNow();
    poller.interrupt();
  }

  @Override
  protected void finalize() {
    this.shutdown();
  }

  private static class DelayedExecutionPoller extends Thread {
    // Store service reference in this static class to prevent a reference of TezSharedExecutor from
    // being held inside a non static class which prevents cleanup via GC.
    private final ThreadPoolExecutor service;

    // A queue which contains instances which have tasks to be executed.
    private final LinkedBlockingQueue<ExecutorServiceInternal> executeQueue =
        new LinkedBlockingQueue<>();

    DelayedExecutionPoller(ThreadPoolExecutor service) {
      super("DelayedExecutionPoller");
      this.setDaemon(true);
      this.service = service;
    }

    void add(ExecutorServiceInternal es) {
      executeQueue.add(es);
    }

    @Override
    public void run() {
      while (!service.isShutdown()) {
        try {
          executeQueue.take().tryExecute();
        } catch (InterruptedException e) {
        }
      }
    }
  }

  /*
   * The internal shared executor service which delegates all the execution to the shared service.
   * It allows managing a given instance of ExecutorService independently of other instances created
   * in the same service.
   *
   * - It stores a queue of submitted tasks and submits only the configured poolSize number of tasks
   *   into the shared executor service.
   * - Stores a list of futures used implement shutdownNow and awaitTermination.
   */
  private class ExecutorServiceInternal extends AbstractExecutorService {
    // This contains all the tasks which are submitted through this ExecutorService and has not
    // finished, we use this to implement shutdownNow and awaitForTermination.
    // Note: This should have been an Set, but we do not have a concurrent set.
    private final ConcurrentHashMap<ManagedFutureTask<?>, Boolean> futures =
        new ConcurrentHashMap<>();

    // Number of tasks currently submitted by this executor to the common executor service.
    private final AtomicInteger numTasksSubmitted = new AtomicInteger();

    // The list of pending tasks to be submitted on behalf of this service.
    private final LinkedBlockingQueue<ManagedFutureTask<?>> pendingTasks =
        new LinkedBlockingQueue<>();

    // Set to 0 when shutdown is complete, a CountDownLatch is used to enable wait for shutdown in
    // awaitTermination.
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    // The thread name to be used for threads executing tasks of this executor.
    private final String threadName;

    // Total number of threads to be used.
    private final int poolSize;

    ExecutorServiceInternal(int poolSize, String threadName) {
      Preconditions.checkArgument(poolSize > 0, "Expected poolSize > 0");
      this.threadName = threadName;
      this.poolSize = poolSize;
    }

    // A FutureTask which we will use to wrap all the runnable and callable. It adds and removes
    // from the futures set above. And also notifies TezSharedExecutor to pick new tasks from the
    // current ExecutorServiceInternal instance.
    private class ManagedFutureTask<V> extends FutureTask<V> {
      // Set to true if this task was submitted to the shared ExecutorService.
      private boolean submitted = false;

      ManagedFutureTask(Runnable runnable, V value) {
        super(runnable, value);
        addFuture(this);
      }

      ManagedFutureTask(Callable<V> callable) {
        super(callable);
        addFuture(this);
      }

      @Override
      public void run() {
        Thread thisThread = Thread.currentThread();
        String savedThreadName = null;
        if (threadName != null) {
          savedThreadName = thisThread.getName();
          thisThread.setName(String.format(threadName, thisThread.getId()));
        }
        try {
          super.run();
        } finally {
          if (threadName != null) {
            thisThread.setName(savedThreadName);
          }
        }
      }

      // There is a race b/w cancel and submit hence the synchronization.
      synchronized void submit() {
        submitted = true;
        service.execute(this);
      }

      @Override
      public void done() {
        removeFuture(this);
        synchronized (this) {
          if (submitted) { // Decrement only if this task was submitted.
            numTasksSubmitted.decrementAndGet();
          }
        }
        // Add internal executor service to poller to schedule another task if available.
        // We do this instead of invoking tryExecute here, to give a chance for this thread to be
        // reused. But its still possible that a new thread is created.
        poller.add(ExecutorServiceInternal.this);
      }
    }

    private void addFuture(ManagedFutureTask<?> future) {
      futures.put(future, Boolean.TRUE);
      // If already shutdown, reject this task.
      if (isShutdown()) {
        service.getRejectedExecutionHandler().rejectedExecution(future, service);
      }
    }

    private void removeFuture(ManagedFutureTask<?> future) {
      futures.remove(future);
    }

    // Return our internal future task so that all the tasks submitted are tracked and cleaned up.
    @SuppressWarnings("unchecked")
    @Override
    protected <T> ManagedFutureTask<T> newTaskFor(Runnable runnable, T value) {
      if (runnable instanceof ManagedFutureTask) {
        return (ManagedFutureTask<T>)runnable;
      }
      return new ManagedFutureTask<T>(runnable, value);
    }

    @Override
    protected <T> ManagedFutureTask<T> newTaskFor(Callable<T> callable) {
      return new ManagedFutureTask<T>(callable);
    }

    @Override
    public void shutdown() {
      shutdownLatch.countDown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdownLatch.countDown();
      List<Runnable> pending = new ArrayList<>(pendingTasks.size());
      pendingTasks.drainTo(pending);
      // cancel all futures, interrupt if its running.
      for (ManagedFutureTask<?> future : futures.keySet()) {
        future.cancel(true);
      }
      return pending;
    }

    @Override
    public boolean isShutdown() {
      return shutdownLatch.getCount() == 0 || service.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      if (!isShutdown()) {
        return false;
      }
      // futures should be empty ideally, but there is a corner case where all the futures are done
      // but not yet removed from futures map, for that case we check if the future is done.
      for (ManagedFutureTask<?> future : futures.keySet()) {
        if (!future.isDone()) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      long deadline = System.nanoTime() + unit.toNanos(timeout);
      // Wait for shutdown to be invoked.
      if (!shutdownLatch.await(timeout, unit)) {
        return false;
      }
      // Wait for the remaining futures to finish.
      for (ManagedFutureTask<?> future : futures.keySet()) {
        long nanosLeft = deadline - System.nanoTime();
        if (nanosLeft <= 0) {
          return false;
        }
        try {
          future.get(nanosLeft, TimeUnit.NANOSECONDS);
        } catch (ExecutionException | CancellationException ignore) {
        } catch (TimeoutException e) {
          return false;
        }
      }
      return true;
    }

    // Submit a task if task is available and poolSize has not been reached.
    private void tryExecute() {
      while (!pendingTasks.isEmpty()) {
        int numTasks = numTasksSubmitted.get();
        if (numTasks >= poolSize) {
          return;
        }
        if (numTasksSubmitted.compareAndSet(numTasks, numTasks + 1)) {
          ManagedFutureTask<?> task = pendingTasks.poll();
          // This breaks a contract unfortunately. If a task is submitted and it ends up in a
          // queue and then the shared service is shutdown then this job cannot be executed, which
          // is not the contract, ideally it should execute the task.
          if (task == null || task.isCancelled() || service.isShutdown()) {
            numTasksSubmitted.decrementAndGet();
          } else {
            task.submit();
          }
        }
      }
    }

    @Override
    public void execute(Runnable command) {
      this.pendingTasks.add(newTaskFor(command, null));
      this.tryExecute();
    }

    @Override
    protected void finalize() {
      this.shutdown();
    }
  }
}
