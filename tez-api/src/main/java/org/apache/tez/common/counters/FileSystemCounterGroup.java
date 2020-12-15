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

package org.apache.tez.common.counters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Joiner;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringInterner;

/**
 * An abstract class to provide common implementation of the filesystem
 * counter group in both mapred and mapreduce packages.
 *
 * @param <C> the type of the Counter for the group
 */
@InterfaceAudience.Private
public abstract class FileSystemCounterGroup<C extends TezCounter>
    implements CounterGroupBase<C> {

  static final int MAX_NUM_SCHEMES = 100; // intern/sanity check
  static final ConcurrentMap<String, String> schemes = Maps.newConcurrentMap();

  // C[] would need Array.newInstance which requires a Class<C> reference.
  // Just a few local casts probably worth not having to carry it around.
  private final Map<String, Object[]> map =
    new ConcurrentSkipListMap<String, Object[]>();
  private String displayName = StringInterner.weakIntern("File System Counters");

  private static final Joiner NAME_JOINER = Joiner.on('_');

  @InterfaceAudience.Private
  public static class FSCounter extends AbstractCounter {
    final String scheme;
    final FileSystemCounter key;
    private long value;

    public FSCounter(String scheme, FileSystemCounter ref) {
      this.scheme = scheme; // this is interned in the checkScheme() method via a map
      key = ref;
    }

    @Override
    public String getName() {
      return NAME_JOINER.join(scheme, key.name());
    }

    @Override
    public String getDisplayName() {
      return getName();
    }

    @Override
    public long getValue() {
      return value;
    }

    @Override
    public void setValue(long value) {
      this.value = value;
    }

    @Override
    public void increment(long incr) {
      value += incr;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      assert false : "shouldn't be called";
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      assert false : "shouldn't be called";
    }

    @Override
    public TezCounter getUnderlyingCounter() {
      return this;
    }
  }

  @Override
  public String getName() {
    return FileSystemCounter.class.getName();
  }

  @Override
  public String getDisplayName() {
    return displayName;
  }

  @Override
  public void setDisplayName(String displayName) {
    this.displayName = StringInterner.weakIntern(displayName);
  }

  @Override
  public void addCounter(C counter) {
    C ours;
    if (counter instanceof FileSystemCounterGroup.FSCounter) {
      FSCounter c = (FSCounter) counter;
      ours = findCounter(c.scheme, c.key);
    }
    else {
      ours = findCounter(counter.getName());
    }
    ours.setValue(counter.getValue());
  }

  @Override
  public C addCounter(String name, String displayName, long value) {
    C counter = findCounter(name);
    counter.setValue(value);
    return counter;
  }

  // Parse generic counter name into [scheme, key]
  private String[] parseCounterName(String counterName) {
    int schemeEnd = counterName.indexOf('_');
    if (schemeEnd < 0) {
      throw new IllegalArgumentException("bad fs counter name");
    }
    return new String[]{counterName.substring(0, schemeEnd),
                        counterName.substring(schemeEnd + 1)};
  }

  @Override
  public C findCounter(String counterName, String displayName) {
    return findCounter(counterName);
  }

  @Override
  public C findCounter(String counterName, boolean create) {
    try {
      String[] pair = parseCounterName(counterName);
      return findCounter(pair[0], FileSystemCounter.valueOf(pair[1]));
    }
    catch (Exception e) {
      if (create) throw new IllegalArgumentException(e);
      return null;
    }
  }

  @Override
  public C findCounter(String counterName) {
    return findCounter(counterName, true);
  }

  @SuppressWarnings("unchecked")
  public synchronized C findCounter(String scheme, FileSystemCounter key) {
    final String canonicalScheme = checkScheme(scheme);
    Object[] counters = map.get(canonicalScheme);
    int ord = key.ordinal();
    if (counters == null) {
      counters = new Object[FileSystemCounter.values().length];
      map.put(canonicalScheme, counters);
      counters[ord] = newCounter(canonicalScheme, key);
    }
    else if (counters[ord] == null) {
      counters[ord] = newCounter(canonicalScheme, key);
    }
    return (C) counters[ord];
  }

  private String checkScheme(String scheme) {
    String fixed = scheme.toUpperCase(Locale.ENGLISH);
    String interned = schemes.putIfAbsent(fixed, fixed);
    if (schemes.size() > MAX_NUM_SCHEMES) {
      // mistakes or abuses
      throw new IllegalArgumentException("too many schemes? "+ schemes.size() +
                                         " when process scheme: "+ scheme);
    }
    return interned == null ? fixed : interned;
  }

  /**
   * Abstract factory method to create a file system counter
   * @param scheme of the file system
   * @param key the enum of the file system counter
   * @return a new file system counter
   */
  protected abstract C newCounter(String scheme, FileSystemCounter key);

  @Override
  public int size() {
    int n = 0;
    for (Object[] counters : map.values()) {
      n += numSetCounters(counters);
    }
    return n;
  }

  @Override
  public void incrAllCounters(CounterGroupBase<C> rightGroup) {
    aggrAllCounters(rightGroup);
  }

  @Override
  public void aggrAllCounters(CounterGroupBase<C> other) {
    if (Objects.requireNonNull(other.getUnderlyingGroup(), "other group")
        instanceof FileSystemCounterGroup<?>) {
      for (TezCounter counter : other) {
        FSCounter c = (FSCounter) ((TezCounter)counter).getUnderlyingCounter();
        findCounter(c.scheme, c.key) .aggregate(counter);
      }
    }
  }

  /**
   * FileSystemGroup ::= #scheme (scheme #counter (key value)*)*
   */
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, map.size()); // #scheme
    for (Map.Entry<String, Object[]> entry : map.entrySet()) {
      WritableUtils.writeString(out, entry.getKey()); // scheme
      // #counter for the above scheme
      WritableUtils.writeVInt(out, numSetCounters(entry.getValue()));
      for (Object counter : entry.getValue()) {
        if (counter == null) continue;
        FSCounter c = (FSCounter) ((TezCounter)counter).getUnderlyingCounter();
        WritableUtils.writeVInt(out, c.key.ordinal());  // key
        WritableUtils.writeVLong(out, c.getValue());    // value
      }
    }
  }

  private int numSetCounters(Object[] counters) {
    int n = 0;
    for (Object counter : counters) if (counter != null) ++n;
    return n;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numSchemes = WritableUtils.readVInt(in);    // #scheme
    FileSystemCounter[] enums = FileSystemCounter.values();
    for (int i = 0; i < numSchemes; ++i) {
      String scheme = WritableUtils.readString(in); // scheme
      int numCounters = WritableUtils.readVInt(in); // #counter
      for (int j = 0; j < numCounters; ++j) {
        findCounter(scheme, enums[WritableUtils.readVInt(in)])  // key
            .setValue(WritableUtils.readVLong(in)); // value
      }
    }
  }

  @Override
  public Iterator<C> iterator() {
    return new AbstractIterator<C>() {
      Iterator<Object[]> it = map.values().iterator();
      Object[] counters = it.hasNext() ? it.next() : null;
      int i = 0;
      @Override
      protected C computeNext() {
        while (counters != null) {
          while (i < counters.length) {
            @SuppressWarnings("unchecked")
            C counter = (C) counters[i++];
            if (counter != null) return counter;
          }
          i = 0;
          counters = it.hasNext() ? it.next() : null;
        }
        return endOfData();
      }
    };
  }

  @Override
  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof CounterGroupBase<?>) {
      @SuppressWarnings("unchecked")
      CounterGroupBase<C> right = (CounterGroupBase<C>) genericRight;
      return Iterators.elementsEqual(iterator(), right.iterator());
    }
    return false;
  }

  @Override
  public synchronized int hashCode() {
    // need to be deep as counters is an array
    int hash = FileSystemCounter.class.hashCode();
    for (Object[] counters : map.values()) {
      if (counters != null) hash ^= Arrays.hashCode(counters);
    }
    return hash;
  }
}
