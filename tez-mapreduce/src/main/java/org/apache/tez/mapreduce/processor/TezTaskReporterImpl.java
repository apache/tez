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

package org.apache.tez.mapreduce.processor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.common.TezTaskUmbilicalProtocol;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.records.TezTaskDependencyCompletionEventsUpdate;

@InterfaceAudience.Private
@InterfaceStability.Unstable 
class TezTaskReporterImpl 
    implements org.apache.tez.common.TezTaskReporter, Runnable {

  private static final Log LOG = LogFactory.getLog(TezTaskReporterImpl.class);
  
  private final MRTask mrTask;
  private final TezTaskUmbilicalProtocol umbilical;
  private final Progress taskProgress;
  
  private Thread pingThread = null;
  private boolean done = true;
  private Object lock = new Object();

  /**
   * flag that indicates whether progress update needs to be sent to parent.
   * If true, it has been set. If false, it has been reset. 
   * Using AtomicBoolean since we need an atomic read & reset method. 
   */  
  private AtomicBoolean progressFlag = new AtomicBoolean(false);
  
  TezTaskReporterImpl(MRTask mrTask, TezTaskUmbilicalProtocol umbilical) {
    this.mrTask = mrTask;
    this.umbilical = umbilical;
    this.taskProgress = mrTask.getProgress();
  }

  // getters and setters for flag
  void setProgressFlag() {
    progressFlag.set(true);
  }
  
  boolean resetProgressFlag() {
    return progressFlag.getAndSet(false);
  }
  
  public void setStatus(String status) {
    // FIXME - BADLY
    if (true) {
      return;
    }
    taskProgress.setStatus(
        MRTask.normalizeStatus(status, this.mrTask.jobConf));
    // indicate that progress update needs to be sent
    setProgressFlag();
  }
  
  public void setProgress(float progress) {
    // set current phase progress.
    // This method assumes that task has phases.
    taskProgress.phase().set(progress);
    // indicate that progress update needs to be sent
    setProgressFlag();
  }
  
  public float getProgress() {
    return taskProgress.getProgress();
  };
  
  public void progress() {
    // indicate that progress update needs to be sent
    setProgressFlag();
  }
  
  public TezCounter getCounter(String group, String name) {
    return this.mrTask.counters == null ? 
        null : 
        this.mrTask.counters.findCounter(group, name);
  }
  
  public TezCounter getCounter(Enum<?> name) {
    return this.mrTask.counters == null ? 
        null : 
        this.mrTask.counters.findCounter(name);
  }
  
  public void incrCounter(Enum<?> key, long amount) {
    if (this.mrTask.counters != null) {
      this.mrTask.counters.findCounter(key).increment(amount);
    }
    setProgressFlag();
  }
  
  public void incrCounter(String group, String counter, long amount) {
    if (this.mrTask.counters != null) {
      this.mrTask.counters.findCounter(group, counter).increment(amount);
    }
    setProgressFlag();
  }
  
  /** 
   * The communication thread handles communication with the parent (Task Tracker). 
   * It sends progress updates if progress has been made or if the task needs to 
   * let the parent know that it's alive. It also pings the parent to see if it's alive. 
   */
  public void run() {
    final int MAX_RETRIES = 3;
    int remainingRetries = MAX_RETRIES;
    // get current flag value and reset it as well
    boolean sendProgress = resetProgressFlag();
    while (!this.mrTask.taskDone.get()) {
      synchronized (lock) {
        done = false;
      }
      try {
        boolean taskFound = true; // whether TT knows about this task
        // sleep for a bit
        synchronized(lock) {
          if (this.mrTask.taskDone.get()) {
            break;
          }
          lock.wait(MRTask.PROGRESS_INTERVAL);
        }
        if (this.mrTask.taskDone.get()) {
          break;
        }

        if (sendProgress) {
          // we need to send progress update
          this.mrTask.updateCounters();
          this.mrTask.getStatus().statusUpdate(
              taskProgress.get(),
              taskProgress.toString(), 
              this.mrTask.counters);

          // broken code now due to tez engine changes
          taskFound = false;
          /*
          taskFound = 
              umbilical.statusUpdate(
                  this.mrTask.getTaskAttemptId(), this.mrTask.getStatus());
           */
          this.mrTask.getStatus().clearStatus();
        }
        else {
          // send ping 
          taskFound = false;
          // broken code now due to tez engine changes
          //umbilical.ping(this.mrTask.getTaskAttemptId());
        }

        // if Task Tracker is not aware of our task ID (probably because it died and 
        // came back up), kill ourselves
        if (!taskFound) {
          MRTask.LOG.warn("Parent died.  Exiting " + this.mrTask.getTaskAttemptId());
          resetDoneFlag();
          System.exit(66);
        }

        sendProgress = resetProgressFlag(); 
        remainingRetries = MAX_RETRIES;
      } 
      catch (Throwable t) {
        MRTask.LOG.info("Communication exception: " + StringUtils.stringifyException(t));
        remainingRetries -=1;
        if (remainingRetries == 0) {
          ReflectionUtils.logThreadInfo(MRTask.LOG, "Communication exception", 0);
          MRTask.LOG.warn("Last retry, killing " + this.mrTask.getTaskAttemptId());
          resetDoneFlag();
          System.exit(65);
        }
      }
    }
    //Notify that we are done with the work
    resetDoneFlag();
  }
  void resetDoneFlag() {
    synchronized (lock) {
      done = true;
      lock.notify();
    }
  }
  public void startCommunicationThread() {
    if (pingThread == null) {
      pingThread = new Thread(this, "communication thread");
      pingThread.setDaemon(true);
      pingThread.start();
    }
  }
  public void stopCommunicationThread() throws InterruptedException {
    if (pingThread != null) {
      // Intent of the lock is to not send an interupt in the middle of an
      // umbilical.ping or umbilical.statusUpdate
      synchronized(lock) {
      //Interrupt if sleeping. Otherwise wait for the RPC call to return.
        lock.notify(); 
      }

      synchronized (lock) { 
        while (!done) {
          lock.wait();
        }
      }
      pingThread.interrupt();
      pingThread.join();
    }
  }

  @Override
  public TezTaskDependencyCompletionEventsUpdate getDependentTasksCompletionEvents(
      int fromEventIdx, int maxEventsToFetch,
      TezTaskAttemptID reduce) {
    return umbilical.getDependentTasksCompletionEvents(
        fromEventIdx, maxEventsToFetch, reduce);
  }

  @Override
  public void reportFatalError(TezTaskAttemptID taskAttemptId,
      Throwable throwable, String logMsg) {
    LOG.fatal(logMsg);
    Throwable tCause = throwable.getCause();
    String cause = tCause == null 
                   ? StringUtils.stringifyException(throwable)
                   : StringUtils.stringifyException(tCause);
/*
                   try {
      umbilical.fatalError(mrTask.getTaskAttemptId(), cause);
    } catch (IOException ioe) {
      LOG.fatal("Failed to contact the tasktracker", ioe);
      System.exit(-1);
    }
    */
  }

  public TezTaskUmbilicalProtocol getUmbilical() {
    return umbilical;
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    // TODO TEZAM3
    return 1;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignature.getProtocolSignature(this, protocol,
        clientVersion, clientMethodsHash);
  }
}
