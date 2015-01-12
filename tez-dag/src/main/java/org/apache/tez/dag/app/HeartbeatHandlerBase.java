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

package org.apache.tez.dag.app;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;

public abstract class HeartbeatHandlerBase<T> extends AbstractService {


  protected int timeOut;
  protected int timeOutCheckInterval;
  protected Thread timeOutCheckerThread;
  private final String name;
  
  @SuppressWarnings("rawtypes")
  protected final EventHandler eventHandler;
  protected final Clock clock;
  protected final AppContext appContext;
  
  private ConcurrentMap<T, ReportTime> runningMap;
  private volatile boolean stopped;

  public HeartbeatHandlerBase(AppContext appContext, int expectedConcurrency, String name) {
    super(name);
    this.name = name;
    this.eventHandler = appContext.getEventHandler();
    this.clock = appContext.getClock();
    this.appContext = appContext;
    expectedConcurrency = expectedConcurrency == 0 ? 1 : expectedConcurrency;
    this.runningMap = new ConcurrentHashMap<T, HeartbeatHandlerBase.ReportTime>(
        16, 0.75f, expectedConcurrency);
  }

  @Override
  public void serviceInit(Configuration conf) {
    timeOut = getConfiguredTimeout(conf);
    timeOutCheckInterval = getConfiguredTimeoutCheckInterval(conf);
  }

  @Override
  public void serviceStart() {
    timeOutCheckerThread = new Thread(createPingChecker());
    timeOutCheckerThread.setName(name + " PingChecker");
    timeOutCheckerThread.start();
  }

  @Override
  public void serviceStop() {
    stopped = true;
    if (timeOutCheckerThread != null) {
      timeOutCheckerThread.interrupt();
    }
  }
  
  protected Runnable createPingChecker() {
    return new PingChecker();
  }
  protected abstract int getConfiguredTimeout(Configuration conf);
  protected abstract int getConfiguredTimeoutCheckInterval(Configuration conf);
  
  public void progressing(T id) {
    ReportTime time = runningMap.get(id);
    if (time != null) {
      time.setLastProgress(clock.getTime());
    }
  }
  
  public void pinged(T id) {
    ReportTime time = runningMap.get(id);
    if (time != null) {
      time.setLastPing(clock.getTime());
    }
  }
  
  public void register(T id) {
    runningMap.put(id, new ReportTime(clock.getTime()));
  }
  
  public void unregister(T id) {
    runningMap.remove(id);
  }
  
  
  
  protected static class ReportTime {
    private long lastPing;
    private long lastProgress;
    
    public ReportTime(long time) {
      setLastProgress(time);
    }
    
    public synchronized void setLastPing(long time) {
      lastPing = time;
    }
    
    public synchronized void setLastProgress(long time) {
      lastProgress = time;
      lastPing = time;
    }
    
    public synchronized long getLastPing() {
      return lastPing;
    }
    
    public synchronized long getLastProgress() {
      return lastProgress;
    }
  }
  
  protected abstract boolean hasTimedOut(ReportTime report, long currentTime);
  
  protected abstract void handleTimeOut(T t);
  
  private class PingChecker implements Runnable {

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        Iterator<Map.Entry<T, ReportTime>> iterator =
            runningMap.entrySet().iterator();

        // avoid calculating current time everytime in loop
        long currentTime = clock.getTime();

        while (iterator.hasNext()) {
          Map.Entry<T, ReportTime> entry = iterator.next();    
          if(hasTimedOut(entry.getValue(), currentTime)) {
            // Timed out. Removed from list and send out an event.
            iterator.remove();
            handleTimeOut(entry.getKey());
          }
        }
        try {
          Thread.sleep(timeOutCheckInterval);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }
  
}
