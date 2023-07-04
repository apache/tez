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

package org.apache.tez.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_REMOTE_APP_LOG_DIR;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_THREAD_DUMP_INTERVAL;
import static org.apache.tez.dag.api.TezConfiguration.TEZ_THREAD_DUMP_INTERVAL_DEFAULT;

public final class TezThreadDumpHelper {

  private final long duration;
  private final Path basePath;
  private final FileSystem fs;

  private static final ThreadMXBean THREAD_BEAN = ManagementFactory.getThreadMXBean();
  private static final Logger LOG = LoggerFactory.getLogger(TezThreadDumpHelper.class);

  private ScheduledExecutorService periodicThreadDumpServiceExecutor;

  private TezThreadDumpHelper(long duration, Path basePath, Configuration conf) throws IOException {
    this.duration = duration;
    this.basePath = basePath;
    this.fs = basePath.getFileSystem(conf);
  }

  public static TezThreadDumpHelper getTezThreadDumpHelper(Configuration conf) {
    long periodicThreadDumpFrequency =
        conf.getTimeDuration(TEZ_THREAD_DUMP_INTERVAL, TEZ_THREAD_DUMP_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);

    if (periodicThreadDumpFrequency > 0) {
      Path basePath = new Path(conf.get(NM_REMOTE_APP_LOG_DIR, DEFAULT_NM_REMOTE_APP_LOG_DIR));
      LOG.info("Periodic Thread Dump Capture Service Configured to capture Thread Dumps at {} ms frequency and at " +
          "path: {}", periodicThreadDumpFrequency, basePath);
      try {
        return new TezThreadDumpHelper(periodicThreadDumpFrequency, basePath, conf);
      } catch (IOException e) {
        LOG.warn("Can not initialize periodic thread dump service", e);
      }

    }
    return null;
  }

  public void schedulePeriodicThreadDumpService(String dagName) {
    periodicThreadDumpServiceExecutor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true).
                setNameFormat("PeriodicThreadDumpService{" + dagName + "} #%d").build());
    Runnable threadDumpCollector = new ThreadDumpCollector(basePath, dagName, fs);
    periodicThreadDumpServiceExecutor.schedule(threadDumpCollector, duration, TimeUnit.MILLISECONDS);
  }

  public void shutdownPeriodicThreadDumpService() {
    if (periodicThreadDumpServiceExecutor != null) {
      periodicThreadDumpServiceExecutor.shutdown();

      try {
        if (!periodicThreadDumpServiceExecutor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
          periodicThreadDumpServiceExecutor.shutdownNow();
        }
      } catch (InterruptedException ignored) {
        // Ignore interrupt, will attempt a final shutdown below.
      }
      periodicThreadDumpServiceExecutor.shutdownNow();
      periodicThreadDumpServiceExecutor = null;
    }
  }

  private static class ThreadDumpCollector implements Runnable {

    private final Path path;
    private final String dagName;
    private final FileSystem fs;

    ThreadDumpCollector(Path path, String dagName, FileSystem fs) {
      this.path = path;
      this.fs = fs;
      this.dagName = dagName;
    }

    @Override
    public void run() {
      if (!Thread.interrupted()) {
        try (FSDataOutputStream fsStream = fs.create(
            new Path(path, dagName + "_" + System.currentTimeMillis() + ".jstack"));
            PrintStream printStream = new PrintStream(fsStream, false, "UTF8")) {
          printThreadInfo(printStream, dagName);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    public synchronized void printThreadInfo(PrintStream stream, String title) {
      boolean contention = THREAD_BEAN.isThreadContentionMonitoringEnabled();
      long[] threadIds = THREAD_BEAN.getAllThreadIds();
      stream.println("Process Thread Dump: " + title);
      stream.println(threadIds.length + " active threads");
      for (long tid : threadIds) {
        ThreadInfo info = THREAD_BEAN.getThreadInfo(tid, Integer.MAX_VALUE);
        if (info == null) {
          stream.println("  Inactive");
          continue;
        }
        stream.println("Thread " + getTaskName(info.getThreadId(), info.getThreadName()) + ":");
        Thread.State state = info.getThreadState();
        stream.println("  State: " + state);
        stream.println("  Blocked count: " + info.getBlockedCount());
        stream.println("  Waited count: " + info.getWaitedCount());
        if (contention) {
          stream.println("  Blocked time: " + info.getBlockedTime());
          stream.println("  Waited time: " + info.getWaitedTime());
        }
        if (state == Thread.State.WAITING) {
          stream.println("  Waiting on " + info.getLockName());
        } else if (state == Thread.State.BLOCKED) {
          stream.println("  Blocked on " + info.getLockName());
          stream.println("  Blocked by " + getTaskName(info.getLockOwnerId(), info.getLockOwnerName()));
        }
        stream.println("  Stack:");
        for (StackTraceElement frame : info.getStackTrace()) {
          stream.println("    " + frame.toString());
        }
      }
      stream.flush();
    }

    private String getTaskName(long id, String name) {
      if (name == null) {
        return Long.toString(id);
      }
      return id + " (" + name + ")";
    }
  }
}
