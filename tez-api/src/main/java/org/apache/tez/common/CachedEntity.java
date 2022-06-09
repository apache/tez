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
package org.apache.tez.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;

/**
 * A thread safe implementation used as a container for cacheable entries with Expiration times.
 * It supports custom {@link Clock} to control the elapsed time calculation.
 * @param <T> the data object type.
 */
public class CachedEntity<T> {
  private final AtomicReference<T> entryDataRef;
  private final Clock cacheClock;
  private final long expiryDurationMS;
  private volatile long entryTimeStamp;

  public CachedEntity(TimeUnit expiryTimeUnit, long expiryLength, Clock clock) {
    entryDataRef = new AtomicReference<>(null);
    cacheClock = clock;
    expiryDurationMS = TimeUnit.MILLISECONDS.convert(expiryLength, expiryTimeUnit);
    entryTimeStamp = 0;
  }

  public CachedEntity(TimeUnit expiryTimeUnit, long expiryLength) {
    this(expiryTimeUnit, expiryLength, new MonotonicClock());
  }

  /**
   *
   * @return true if expiration timestamp is 0, or the elapsed time since last update is
   *         greater than {@link #expiryDurationMS}
   */
  public boolean isExpired() {
    return (entryTimeStamp == 0)
        || ((cacheClock.getTime() - entryTimeStamp) > expiryDurationMS);
  }

  /**
   * If the entry has expired, it reset the cache reference through {@link #clearExpiredEntry()}.
   * @return cached data if the timestamp is valid. Null, if the timestamp has expired.
   */
  public T getValue() {
    if (isExpired()) { // quick check for expiration
      if (clearExpiredEntry()) { // remove reference to the expired entry
        return null;
      }
    }
    return entryDataRef.get();
  }

  /**
   * Safely sets the cached data.
   * @param newEntry
   */
  public void setValue(T newEntry) {
    T currentEntry = entryDataRef.get();
    while (!entryDataRef.compareAndSet(currentEntry, newEntry)) {
      currentEntry = entryDataRef.get();
    }
    entryTimeStamp = cacheClock.getTime();
  }

  /**
   * Enforces the expiration of the cached entry.
   */
  public void enforceExpiration() {
    entryTimeStamp = 0;
  }

  /**
   * Safely deletes the reference to the data if it was not null.
   * @return true if the reference is set to Null. False indicates that another thread
   *         updated the cache.
   */
  private boolean clearExpiredEntry() {
    T currentEntry = entryDataRef.get();
    if (currentEntry == null) {
      return true;
    }
    // the current value is not null: try to reset it.
    // if the CAS is successful, then we won't override a recent update to the cache.
    return (entryDataRef.compareAndSet(currentEntry, null));
  }
}
