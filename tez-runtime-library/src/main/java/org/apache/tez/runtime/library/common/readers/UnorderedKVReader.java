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

package org.apache.tez.runtime.library.common.readers;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.IOInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.InMemoryReader;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput.Type;
import org.apache.tez.runtime.library.common.shuffle.MemoryFetchedInput;

@Unstable
@Private
public class UnorderedKVReader<K, V> extends KeyValueReader {

  private static final Logger LOG = LoggerFactory.getLogger(UnorderedKVReader.class);
  
  private final ShuffleManager shuffleManager;
  private final CompressionCodec codec;
  
  private final Class<K> keyClass;
  private final Class<V> valClass;
  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valDeserializer;
  private final DataInputBuffer keyIn;
  private final DataInputBuffer valIn;

  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;
  private final int ifileBufferSize;
  
  private final TezCounter inputRecordCounter;
  private final InputContext context;
  
  private K key;
  private V value;
  
  private FetchedInput currentFetchedInput;
  private IFile.Reader currentReader;
  
  // TODO Remove this once per I/O counters are separated properly. Relying on
  // the counter at the moment will generate aggregate numbers. 
  private int numRecordsRead = 0;
  private final AtomicLong totalBytesRead = new AtomicLong(0);
  private final AtomicLong totalFileBytes = new AtomicLong(0);


  public UnorderedKVReader(ShuffleManager shuffleManager, Configuration conf,
      CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength, int ifileBufferSize,
      TezCounter inputRecordCounter, InputContext context)
      throws IOException {
    this.shuffleManager = shuffleManager;
    this.context = context;
    this.codec = codec;
    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    this.ifileBufferSize = ifileBufferSize;
    this.inputRecordCounter = inputRecordCounter;

    this.keyClass = ConfigUtils.getIntermediateInputKeyClass(conf);
    this.valClass = ConfigUtils.getIntermediateInputValueClass(conf);

    this.keyIn = new DataInputBuffer();
    this.valIn = new DataInputBuffer();

    SerializationFactory serializationFactory = new SerializationFactory(conf);

    this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
    this.keyDeserializer.open(keyIn);
    this.valDeserializer = serializationFactory.getDeserializer(valClass);
    this.valDeserializer.open(valIn);
  }

  // TODO NEWTEZ Maybe add an interface to check whether next will block.
  
  /**
   * Moves to the next key/values(s) pair
   * 
   * @return true if another key/value(s) pair exists, false if there are no
   *         more.
   * @throws IOException
   *           if an error occurs
   */
  @Override  
  public boolean next() throws IOException {
    if (readNextFromCurrentReader()) {
      inputRecordCounter.increment(1);
      context.notifyProgress();
      numRecordsRead++;
      return true;
    } else {
      boolean nextInputExists = moveToNextInput();
      while (nextInputExists) {
        if(readNextFromCurrentReader()) {
          inputRecordCounter.increment(1);
          context.notifyProgress();
          numRecordsRead++;
          return true;
        }
        nextInputExists = moveToNextInput();
      }
      LOG.info("Num Records read: " + numRecordsRead);
      completedProcessing = true;
      return false;
    }
  }


  @Override
  public Object getCurrentKey() throws IOException {
    return (Object) key;
  }

  @Override
  public Object getCurrentValue() throws IOException {
    return value;
  }

  public float getProgress() throws IOException, InterruptedException {
    final int numInputs = shuffleManager.getNumInputs();
    if (totalFileBytes.get() > 0 && numInputs > 0) {
      return ((1.0f) * (totalBytesRead.get() + ((currentReader != null) ? currentReader.bytesRead :
      0.0f)) /
          totalFileBytes.get()) * (shuffleManager.getNumCompletedInputsFloat() /
          (1.0f * numInputs));
    }
    return 0.0f;
  }
  /**
   * Tries reading the next key and value from the current reader.
   * @return true if the current reader has more records
   * @throws IOException
   */
  private boolean readNextFromCurrentReader() throws IOException {
    // Initial reader.
    if (this.currentReader == null) {
      return false;
    } else {
      boolean hasMore = this.currentReader.nextRawKey(keyIn);
      if (hasMore) {
        this.currentReader.nextRawValue(valIn);
        this.key = keyDeserializer.deserialize(this.key);
        this.value = valDeserializer.deserialize(this.value);
        return true;
      }
      return false;
    }
  }
  
  /**
   * Moves to the next available input. This method may block if the input is not ready yet.
   * Also takes care of closing the previous input.
   * 
   * @return true if the next input exists, false otherwise
   * @throws IOException
   */
  private boolean moveToNextInput() throws IOException {
    if (currentReader != null) { // Close the current reader.
      totalBytesRead.getAndAdd(currentReader.bytesRead);
      currentReader.close();
      /**
       * clear reader explicitly. Otherwise this could point to stale reference when next() is
       * called and end up throwing EOF exception from IFIle. Ref: TEZ-2348
       */
      currentReader = null;
      currentFetchedInput.free();
    }
    try {
      currentFetchedInput = shuffleManager.getNextInput();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for next available input", e);
      Thread.currentThread().interrupt();
      throw new IOInterruptedException(e);
    }
    if (currentFetchedInput == null) {
      hasCompletedProcessing();
      return false; // No more inputs
    } else {
      currentReader = openIFileReader(currentFetchedInput);
      totalFileBytes.getAndAdd(currentReader.getLength());
      return true;
    }
  }

  public IFile.Reader openIFileReader(FetchedInput fetchedInput)
      throws IOException {
    if (fetchedInput.getType() == Type.MEMORY) {
      MemoryFetchedInput mfi = (MemoryFetchedInput) fetchedInput;

      return new InMemoryReader(null, mfi.getInputAttemptIdentifier(),
          mfi.getBytes(), 0, (int) mfi.getSize());
    } else {
      return new IFile.Reader(fetchedInput.getInputStream(),
          fetchedInput.getSize(), codec, null, null, ifileReadAhead,
          ifileReadAheadLength, ifileBufferSize);
    }
  }
}
