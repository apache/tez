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
package org.apache.tez.engine.common.combine;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.common.sort.impl.TezRawKeyValueIterator;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class CombineInput implements Input {

  private final TezRawKeyValueIterator input;
  private TezCounter inputValueCounter;
  private TezCounter inputKeyCounter;
  private RawComparator<Object> comparator;
  private Object key;                                  // current key
  private Object value;                              // current value
  private boolean firstValue = false;                 // first value in key
  private boolean nextKeyIsSame = false;              // more w/ this key
  private boolean hasMore;                            // more in file
  protected Progressable reporter;
  private Deserializer keyDeserializer;
  private Deserializer valueDeserializer;
  private DataInputBuffer buffer = new DataInputBuffer();
  private BytesWritable currentRawKey = new BytesWritable();
  private ValueIterable iterable = new ValueIterable();
  
  public CombineInput(TezRawKeyValueIterator kvIter) {
    this.input = kvIter;
  }

  public void initialize(Configuration conf, Master master) throws IOException,
      InterruptedException {
  }

  public boolean hasNext() throws IOException, InterruptedException {
    while (hasMore && nextKeyIsSame) {
      nextKeyValue();
    }
    if (hasMore) {
      if (inputKeyCounter != null) {
        inputKeyCounter.increment(1);
      }
      return nextKeyValue();
    } else {
      return false;
    }
  }

  private boolean nextKeyValue() throws IOException, InterruptedException {
    if (!hasMore) {
      key = null;
      value = null;
      return false;
    }
    firstValue = !nextKeyIsSame;
    DataInputBuffer nextKey = input.getKey();
    currentRawKey.set(nextKey.getData(), nextKey.getPosition(), 
                      nextKey.getLength() - nextKey.getPosition());
    buffer.reset(currentRawKey.getBytes(), 0, currentRawKey.getLength());
    key = keyDeserializer.deserialize(key);
    DataInputBuffer nextVal = input.getValue();
    buffer.reset(nextVal.getData(), nextVal.getPosition(), nextVal.getLength());
    value = valueDeserializer.deserialize(value);

    hasMore = input.next();
    if (hasMore) {
      nextKey = input.getKey();
      nextKeyIsSame = comparator.compare(currentRawKey.getBytes(), 0, 
                                     currentRawKey.getLength(),
                                     nextKey.getData(),
                                     nextKey.getPosition(),
                                     nextKey.getLength() - nextKey.getPosition()
                                         ) == 0;
    } else {
      nextKeyIsSame = false;
    }
    inputValueCounter.increment(1);
    return true;
  }

  public Object getNextKey() throws IOException, InterruptedException {
    return key;
  }

  public Iterable getNextValues() throws IOException,
      InterruptedException {
    return iterable;
  }

  public float getProgress() throws IOException, InterruptedException {
    return input.getProgress().getProgress();
  }

  public void close() throws IOException {
    input.close();
  }
  
  protected class ValueIterator implements Iterator<Object> {


    public boolean hasNext() {
      return firstValue || nextKeyIsSame;
    }

    public Object next() {

      // if this is the first record, we don't need to advance
      if (firstValue) {
        firstValue = false;
        return value;
      }
      // if this isn't the first record and the next key is different, they
      // can't advance it here.
      if (!nextKeyIsSame) {
        throw new NoSuchElementException("iterate past last value");
      }
      // otherwise, go to the next key/value pair
      try {
        nextKeyValue();
        return value;
      } catch (IOException ie) {
        throw new RuntimeException("next value iterator failed", ie);
      } catch (InterruptedException ie) {
        // this is bad, but we can't modify the exception list of java.util
        throw new RuntimeException("next value iterator interrupted", ie);        
      }
    }

    public void remove() {
      throw new UnsupportedOperationException("remove not implemented");
    }
  }


  
  protected class ValueIterable implements Iterable<Object> {
    private ValueIterator iterator = new ValueIterator();
    public Iterator<Object> iterator() {
      return iterator;
    } 
  }
  


}
