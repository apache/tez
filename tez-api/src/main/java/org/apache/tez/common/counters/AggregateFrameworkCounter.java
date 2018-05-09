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

package org.apache.tez.common.counters;

import org.apache.tez.common.counters.FrameworkCounterGroup.FrameworkCounter;

@SuppressWarnings("rawtypes")
public class AggregateFrameworkCounter<T extends Enum<T>> extends FrameworkCounter implements AggregateTezCounter  {
  
  private long min = Long.MAX_VALUE;
  private long max = Long.MIN_VALUE;
  private long count = 0;

  @SuppressWarnings("unchecked")
  public AggregateFrameworkCounter(Enum<T> ref, String groupName) {
    super(ref, groupName);
  }

  @Override
  public void increment(long incr) {
    throw new IllegalArgumentException("Cannot increment an aggregate counter directly");
  }
  
  @Override
  public void aggregate(TezCounter other) {
    final long val = other.getValue();
    final long othermax;
    final long othermin;
    final long othercount;
    if (other instanceof AggregateTezCounter) {
      othermax = ((AggregateTezCounter) other).getMax();
      othermin = ((AggregateTezCounter) other).getMin();
      othercount = ((AggregateTezCounter) other).getCount();
    } else {
      othermin = othermax = val;
      othercount = 1;
    }
    this.count += othercount;
    super.increment(val);
    if (this.min == Long.MAX_VALUE) {
      this.min = othermin;
      this.max = othermax;
      return;
    }
    this.min = Math.min(this.min, othermin);
    this.max = Math.max(this.max, othermax);
  }

  @Override
  public long getMin() {
    return min;
  }

  @Override
  public long getMax() {
    return max;
  }
  
  @SuppressWarnings("unchecked")
  public FrameworkCounter<T> asFrameworkCounter() {
    return ((FrameworkCounter<T>)this);
  }

  @Override
  public long getCount() {
    return count;
  }

}
