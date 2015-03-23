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

package org.apache.tez.mapreduce.hadoop.mapred;

import static org.apache.hadoop.mapreduce.util.CountersStrings.toEscapedCompactString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.collections.IteratorUtils;

public class MRCounters extends org.apache.hadoop.mapred.Counters {
  private final org.apache.tez.common.counters.TezCounters raw;
  
  public MRCounters(org.apache.tez.common.counters.TezCounters raw) {
    this.raw = raw;
  }

  @Override
  public synchronized org.apache.hadoop.mapred.Counters.Group getGroup(String groupName) {
    return new MRCounterGroup(raw.getGroup(groupName));
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized Collection<String> getGroupNames() {
    return IteratorUtils.toList(raw.getGroupNames().iterator());  }

  @Override
  public synchronized String makeCompactString() {
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for(Group group: this){
      for(Counter counter: group) {
        if (first) {
          first = false;
        } else {
          builder.append(',');
        }
        builder.append(group.getDisplayName());
        builder.append('.');
        builder.append(counter.getDisplayName());
        builder.append(':');
        builder.append(counter.getCounter());
      }
    }
    return builder.toString();
  }

  @Override
  public synchronized Counter findCounter(String group, String name) {
    return new MRCounter(raw.findCounter(group, name));
  }

  @Override
  public Counter findCounter(String group, int id, String name) {
    return new MRCounter(raw.findCounter(group, name));
  }

  @Override
  public void incrCounter(Enum<?> key, long amount) {
    raw.findCounter(key).increment(amount);
  }

  @Override
  public void incrCounter(String group, String counter, long amount) {
    raw.findCounter(group, counter).increment(amount);
  }

  @Override
  public synchronized long getCounter(Enum<?> key) {
    return raw.findCounter(key).getValue();
  }

  @Override
  public synchronized void incrAllCounters(
      org.apache.hadoop.mapred.Counters other) {
    for (Group otherGroup: other) {
      Group group = getGroup(otherGroup.getName());
      group.setDisplayName(otherGroup.getDisplayName());
      for (Counter otherCounter : otherGroup) {
        Counter counter = group.getCounterForName(otherCounter.getName());
        counter.setDisplayName(otherCounter.getDisplayName());
        counter.increment(otherCounter.getValue());
      }
    }
  }
  
  @Override
  public int size() {
    return countCounters();
  }

  @Override
  public String makeEscapedCompactString() {
    return toEscapedCompactString(this);
  }
  
  public static class MRCounterGroup extends org.apache.hadoop.mapred.Counters.Group {
    private final org.apache.tez.common.counters.CounterGroup group;
    public MRCounterGroup(org.apache.tez.common.counters.CounterGroup group) {
      this.group = group;
    }
    @Override
    public String getName() {
      return group.getName();
    }
    @Override
    public String getDisplayName() {
      return group.getDisplayName();
    }
    @Override
    public void setDisplayName(String displayName) {
      group.setDisplayName(displayName);
    }
    @Override
    public void addCounter(org.apache.hadoop.mapred.Counters.Counter counter) {
      group.addCounter(convert(counter));
    }
    @Override
    public org.apache.hadoop.mapred.Counters.Counter addCounter(String name,
        String displayName, long value) {
      return new MRCounter(group.addCounter(name, displayName, value));
    }
    @Override
    public org.apache.hadoop.mapred.Counters.Counter findCounter(
        String counterName, String displayName) {
      return new MRCounter(group.findCounter(counterName, displayName));
    }
    @Override
    public int size() {
      return group.size();
    }
    @Override
    public void incrAllCounters(
        org.apache.hadoop.mapreduce.counters.CounterGroupBase rightGroup) {
      new MRCounterGroup(group).incrAllCounters(rightGroup);
    }
    @Override
    public org.apache.hadoop.mapreduce.counters.CounterGroupBase 
    getUnderlyingGroup() {
      return new MRCounterGroup(group).getUnderlyingGroup();
    }
    @Override
    public void readFields(DataInput arg0) throws IOException {
    }
    @Override
    public void write(DataOutput arg0) throws IOException {
    }
    @Override
    public Iterator iterator() {
      // FIXME?
      return group.iterator();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }

      MRCounterGroup counters = (MRCounterGroup) o;

      if (group != null ? !group.equals(counters.group) : counters.group != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 3491 * result + (group != null ? group.hashCode() : 0);
      return result;
    }
  }
  
  public static class MRCounter extends Counter {
    private final org.apache.tez.common.counters.TezCounter raw;
    
    public MRCounter(org.apache.tez.common.counters.TezCounter raw) {
      this.raw = raw;
    }

    @Override
    public void setDisplayName(String displayName) {
      // TODO Auto-generated method stub
      raw.setDisplayName(displayName);
    }

    @Override
    public String getName() {
      return raw.getName();
    }

    @Override
    public String getDisplayName() {
      return raw.getDisplayName();
    }

    @Override
    public long getValue() {
      return raw.getValue();
    }

    @Override
    public void setValue(long value) {
      raw.setValue(value);
    }

    @Override
    public void increment(long incr) {
      raw.increment(incr);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      raw.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      raw.readFields(in);
    }

    @Override
    public String makeEscapedCompactString() {
      return toEscapedCompactString(new MRCounter(raw));
    }

    @Deprecated
    public boolean contentEquals(Counter counter) {
      MRCounter c = new MRCounter(raw);
      return c.equals(counter.getUnderlyingCounter());
    }


    @Override
    public long getCounter() {
      return raw.getValue();
    }

    @Override
    public org.apache.hadoop.mapreduce.Counter getUnderlyingCounter() {
      return new MRCounter(raw).getUnderlyingCounter();
    }

    @Override
    public synchronized boolean equals(Object genericRight) {
      return raw.equals(genericRight);
    }

    @Override
    public int hashCode() {
      // TODO Auto-generated method stub
      return raw.hashCode();
    }
  }
  
  static org.apache.tez.common.counters.TezCounter convert(
      org.apache.hadoop.mapred.Counters.Counter counter) {
    org.apache.hadoop.mapreduce.Counter underlyingCounter =
        counter.getUnderlyingCounter();
    if (underlyingCounter instanceof org.apache.hadoop.mapreduce.counters.FrameworkCounterGroup.FrameworkCounter) {
      org.apache.hadoop.mapreduce.counters.FrameworkCounterGroup.FrameworkCounter 
      real = 
      (org.apache.hadoop.mapreduce.counters.FrameworkCounterGroup.FrameworkCounter)underlyingCounter;
      return new org.apache.tez.common.counters.FrameworkCounterGroup.FrameworkCounter(
          real.getKey(), real.getGroupName());
    } else if (underlyingCounter instanceof org.apache.hadoop.mapreduce.counters.FileSystemCounterGroup.FSCounter) {
      org.apache.hadoop.mapreduce.counters.FileSystemCounterGroup.FSCounter real = 
          (org.apache.hadoop.mapreduce.counters.FileSystemCounterGroup.FSCounter)underlyingCounter;
      return new org.apache.tez.common.counters.FileSystemCounterGroup.FSCounter(
          real.getScheme(), convert(real.getFileSystemCounter()));
    } else {
      return new org.apache.tez.common.counters.GenericCounter(
          underlyingCounter.getName(), 
          underlyingCounter.getDisplayName(), 
          underlyingCounter.getValue());
    }
  }
  
  static org.apache.tez.common.counters.FileSystemCounter convert(
      org.apache.hadoop.mapreduce.FileSystemCounter c) {
    switch (c) {
      case BYTES_READ:
        return org.apache.tez.common.counters.FileSystemCounter.BYTES_READ;
      case BYTES_WRITTEN:
        return org.apache.tez.common.counters.FileSystemCounter.BYTES_WRITTEN;
      case READ_OPS:
        return org.apache.tez.common.counters.FileSystemCounter.READ_OPS;
      case LARGE_READ_OPS:
        return org.apache.tez.common.counters.FileSystemCounter.LARGE_READ_OPS;
      case WRITE_OPS:
        return org.apache.tez.common.counters.FileSystemCounter.WRITE_OPS;
      default:
        throw new IllegalArgumentException("Unknow FileSystemCounter: " + c);
    }
    
  }
}
