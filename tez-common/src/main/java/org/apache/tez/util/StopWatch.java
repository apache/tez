/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.util;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * A simplified StopWatch implementation which can measure times in nanoseconds.
 */
public class StopWatch implements Closeable {
  private boolean isStarted;
  private long startNanos;
  private long currentElapsedNanos;

  public StopWatch() {
  }

  /**
   * The method is used to find out if the StopWatch is started.
   * @return boolean If the StopWatch is started.
   */
  public boolean isRunning() {
    return isStarted;
  }

  /**
   * Start to measure times and make the state of stopwatch running.
   * @return this instance of StopWatch.
   */
  public StopWatch start() {
    if (isStarted) {
      throw new IllegalStateException("StopWatch is already running");
    }
    isStarted = true;
    startNanos = System.nanoTime();
    return this;
  }

  /**
   * Stop elapsed time and make the state of stopwatch stop.
   * @return this instance of StopWatch.
   */
  public StopWatch stop() {
    if (!isStarted) {
      throw new IllegalStateException("StopWatch is already stopped");
    }
    long now = System.nanoTime();
    isStarted = false;
    currentElapsedNanos += now - startNanos;
    return this;
  }

  /**
   * Reset elapsed time to zero and make the state of stopwatch stop.
   * @return this instance of StopWatch.
   */
  public StopWatch reset() {
    currentElapsedNanos = 0;
    isStarted = false;
    return this;
  }

  /**
   * @return current elapsed time in specified timeunit.
   */
  public long now(TimeUnit timeUnit) {
    return timeUnit.convert(now(), TimeUnit.NANOSECONDS);
  }

  /**
   * @return current elapsed time in nanosecond.
   */
  public long now() {
    return isStarted ?
        System.nanoTime() - startNanos + currentElapsedNanos :
        currentElapsedNanos;
  }

  @Override
  public String toString() {
    return String.valueOf(now());
  }

  @Override
  public void close() {
    if (isStarted) {
      stop();
    }
  }
}
