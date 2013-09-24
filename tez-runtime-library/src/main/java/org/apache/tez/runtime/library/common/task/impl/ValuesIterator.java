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

package org.apache.tez.runtime.library.common.task.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;


/**
 * Iterates values while keys match in sorted input.
 *
 * Usage: Call moveToNext to move to the next k, v pair. This returns true if another exists,
 * followed by getKey() and getValues() to get the current key and list of values.
 * 
 */
public class ValuesIterator<KEY,VALUE> implements Iterator<VALUE> {
  protected TezRawKeyValueIterator in; //input iterator
  private KEY key;               // current key
  private KEY nextKey;
  private VALUE value;             // current value
  private boolean hasNext;                      // more w/ this key
  private boolean more;                         // more in file
  private RawComparator<KEY> comparator;
  protected Progressable reporter;
  private Deserializer<KEY> keyDeserializer;
  private Deserializer<VALUE> valDeserializer;
  private DataInputBuffer keyIn = new DataInputBuffer();
  private DataInputBuffer valueIn = new DataInputBuffer();
  
  public ValuesIterator (TezRawKeyValueIterator in, 
                         RawComparator<KEY> comparator, 
                         Class<KEY> keyClass,
                         Class<VALUE> valClass, Configuration conf, 
                         Progressable reporter)
    throws IOException {
    this.in = in;
    this.comparator = comparator;
    this.reporter = reporter;
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
    this.keyDeserializer.open(keyIn);
    this.valDeserializer = serializationFactory.getDeserializer(valClass);
    this.valDeserializer.open(this.valueIn);
    readNextKey();
    key = nextKey;
    nextKey = null; // force new instance creation
    hasNext = more;
  }

  TezRawKeyValueIterator getRawIterator() { return in; }
  
  /// Iterator methods

  public boolean hasNext() { return hasNext; }

  private int ctr = 0;
  public VALUE next() {
    if (!hasNext) {
      throw new NoSuchElementException("iterate past last value");
    }
    try {
      readNextValue();
      readNextKey();
    } catch (IOException ie) {
      throw new RuntimeException("problem advancing post rec#"+ctr, ie);
    }
    reporter.progress();
    return value;
  }

  public void remove() { throw new RuntimeException("not implemented"); }

  /// Auxiliary methods

  /** Start processing next unique key. */
  public void nextKey() throws IOException {
    // read until we find a new key
    while (hasNext) { 
      readNextKey();
    }
    ++ctr;
    
    // move the next key to the current one
    KEY tmpKey = key;
    key = nextKey;
    nextKey = tmpKey;
    hasNext = more;
  }

  /** True iff more keys remain. */
  public boolean more() { 
    return more; 
  }

  /** The current key. */
  public KEY getKey() { 
    return key; 
  }

  /** 
   * read the next key 
   */
  private void readNextKey() throws IOException {
    more = in.next();
    if (more) {
      DataInputBuffer nextKeyBytes = in.getKey();
      keyIn.reset(nextKeyBytes.getData(), nextKeyBytes.getPosition(), nextKeyBytes.getLength());
      nextKey = keyDeserializer.deserialize(nextKey);
      hasNext = key != null && (comparator.compare(key, nextKey) == 0);
    } else {
      hasNext = false;
    }
  }

  /**
   * Read the next value
   * @throws IOException
   */
  private void readNextValue() throws IOException {
    DataInputBuffer nextValueBytes = in.getValue();
    valueIn.reset(nextValueBytes.getData(), nextValueBytes.getPosition(), nextValueBytes.getLength());
    value = valDeserializer.deserialize(value);
  }
}
