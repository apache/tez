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

package org.apache.tez.runtime.library.common;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;

import com.google.common.base.Preconditions;

/**
 * Iterates values while keys match in sorted input.
 * 
 * This class is not thread safe. Accessing methods from multiple threads will
 * lead to corrupt data.
 * 
 */

@Private
public class ValuesIterator<KEY,VALUE> {
  private static final Log LOG = LogFactory.getLog(ValuesIterator.class.getName());
  protected TezRawKeyValueIterator in; //input iterator
  private KEY key;               // current key
  private KEY nextKey;
  private VALUE value;             // current value
  //private boolean hasNext;                      // more w/ this key
  private boolean more;                         // more in file
  private RawComparator<KEY> comparator;
  private Deserializer<KEY> keyDeserializer;
  private Deserializer<VALUE> valDeserializer;
  private DataInputBuffer keyIn = new DataInputBuffer();
  private DataInputBuffer valueIn = new DataInputBuffer();
  private TezCounter inputKeyCounter;
  private TezCounter inputValueCounter;
  
  private int keyCtr = 0;
  private boolean hasMoreValues; // For the current key.
  private boolean isFirstRecord = true;
  
  public ValuesIterator (TezRawKeyValueIterator in, 
                         RawComparator<KEY> comparator, 
                         Class<KEY> keyClass,
                         Class<VALUE> valClass, Configuration conf,
                         TezCounter inputKeyCounter,
                         TezCounter inputValueCounter)
    throws IOException {
    this.in = in;
    this.comparator = comparator;
    this.inputKeyCounter = inputKeyCounter;
    this.inputValueCounter = inputValueCounter;
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
    this.keyDeserializer.open(keyIn);
    this.valDeserializer = serializationFactory.getDeserializer(valClass);
    this.valDeserializer.open(this.valueIn);
    LOG.info("keyDeserializer=" + keyDeserializer + "; valueDeserializer=" + valDeserializer);
  }

  TezRawKeyValueIterator getRawIterator() { return in; }

  /**
   * Move to the next K-Vs pair
   * @return true if another pair exists, otherwise false.
   * @throws IOException 
   */
  public boolean moveToNext() throws IOException {
    if (isFirstRecord) {
      readNextKey();
      key = nextKey;
      nextKey = null;
      hasMoreValues = more;
      isFirstRecord = false;
    } else {
      nextKey();
    }
    return more;
  }
  
  /** The current key. */
  public KEY getKey() { 
    return key; 
  }
  
  // TODO NEWTEZ Maybe add another method which returns an iterator instead of iterable
  
  public Iterable<VALUE> getValues() {
    return new Iterable<VALUE>() {

      @Override
      public Iterator<VALUE> iterator() {
        
        return new Iterator<VALUE>() {

          private final int keyNumber = keyCtr;
          
          @Override
          public boolean hasNext() {
            return hasMoreValues;
          }

          @Override
          public VALUE next() {
            if (!hasMoreValues) {
              throw new NoSuchElementException("iterate past last value");
            }
            Preconditions
                .checkState(
                    keyNumber == keyCtr,
                    "Cannot use values iterator on the previous K-V pair after moveToNext has been invoked to move to the next K-V pair");
            
            try {
              readNextValue();
              readNextKey();
            } catch (IOException ie) {
              throw new RuntimeException("problem advancing post rec#"+keyCtr, ie);
            }
            inputValueCounter.increment(1);
            return value;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("Cannot remove elements");
          }
        };
      }
    };
  }
  
  

  /** Start processing next unique key. */
  private void nextKey() throws IOException {
    // read until we find a new key
    while (hasMoreValues) { 
      readNextKey();
    }
    if (more) {
      inputKeyCounter.increment(1);
      ++keyCtr;
    }
    
    // move the next key to the current one
    KEY tmpKey = key;
    key = nextKey;
    nextKey = tmpKey;
    hasMoreValues = more;
  }

  /** 
   * read the next key - which may be the same as the current key.
   */
  private void readNextKey() throws IOException {
    more = in.next();
    if (more) {      
      DataInputBuffer nextKeyBytes = in.getKey();
     keyIn.reset(nextKeyBytes.getData(), nextKeyBytes.getPosition(),
         nextKeyBytes.getLength() - nextKeyBytes.getPosition());
      nextKey = keyDeserializer.deserialize(nextKey);
      // TODO Is a counter increment required here ?
      hasMoreValues = key != null && (comparator.compare(key, nextKey) == 0);
    } else {
      hasMoreValues = false;
    }
  }

  /**
   * Read the next value
   * @throws IOException
   */
  private void readNextValue() throws IOException {
    DataInputBuffer nextValueBytes = in.getValue();
    valueIn.reset(nextValueBytes.getData(), nextValueBytes.getPosition(),
        nextValueBytes.getLength() - nextValueBytes.getPosition());
    value = valDeserializer.deserialize(value);
  }
}
