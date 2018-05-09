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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AggregateTezCounterDelegate<T extends TezCounter> extends AbstractCounter implements AggregateTezCounter {

  private final T child;
  private long min = Long.MAX_VALUE;
  private long max = Long.MIN_VALUE;
  private long count = 0;

  public AggregateTezCounterDelegate(T child) {
    this.child = child;
  }
  
  @Override
  public String getName() {
    return child.getName(); // this is a pass-through
  }

  @Override
  public String getDisplayName() {
    return child.getDisplayName();
  }

  @Override
  public long getValue() {
    return child.getValue();
  }

  @Override
  public void setValue(long value) {
    this.child.setValue(value);
  }

  @Override
  public void increment(long incr) {
    throw new UnsupportedOperationException("Cannot increment an aggregate counter");
  }
  
  /* (non-Javadoc)
   * @see org.apache.tez.common.counters.AggregateTezCounter#aggregate(org.apache.tez.common.counters.TezCounter)
   */
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
    this.child.increment(val);
    if (this.min == Long.MAX_VALUE) {
      this.min = othermin;
      this.max = othermax;
      return;
    }
    this.min = Math.min(this.min, othermin);
    this.max = Math.max(this.max, othermax);
  }

  @Override
  public TezCounter getUnderlyingCounter() {
    return this.child;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    throw new UnsupportedOperationException("Cannot deserialize an aggregate counter");
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    throw new UnsupportedOperationException("Cannot deserialize an aggregate counter");
  }

  @Override
  public long getMin() {
    return min;
  }

  @Override
  public long getMax() {
    return max;
  }

  @Override
  public long getCount() {
    return count;
  }
}
