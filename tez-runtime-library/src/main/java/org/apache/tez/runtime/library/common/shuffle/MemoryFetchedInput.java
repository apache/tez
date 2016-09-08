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

package org.apache.tez.runtime.library.common.shuffle;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.tez.common.io.NonSyncByteArrayInputStream;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

import com.google.common.base.Preconditions;

public class MemoryFetchedInput extends FetchedInput {

  private BoundedByteArrayOutputStream byteStream;

  public MemoryFetchedInput(long actualSize, long compressedSize,
      InputAttemptIdentifier inputAttemptIdentifier,
      FetchedInputCallback callbackHandler) {
    super(Type.MEMORY, actualSize, compressedSize, inputAttemptIdentifier, callbackHandler);
    this.byteStream = new BoundedByteArrayOutputStream((int) actualSize);
  }

  @Override
  public OutputStream getOutputStream() {
    return byteStream;
  }

  @Override
  public InputStream getInputStream() {
    return new NonSyncByteArrayInputStream(byteStream.getBuffer());
  }

  public byte[] getBytes() {
    return byteStream.getBuffer();
  }
  
  @Override
  public void commit() {
    if (state == State.PENDING) {
      state = State.COMMITTED;
      notifyFetchComplete();
    }
  }

  @Override
  public void abort() {
    if (state == State.PENDING) {
      state = State.ABORTED;
      notifyFetchFailure();
    }
  }
  
  @Override
  public void free() {
    Preconditions.checkState(
        state == State.COMMITTED || state == State.ABORTED,
        "FetchedInput can only be freed after it is committed or aborted");
    if (state == State.COMMITTED) { // ABORTED would have already called cleanup
      state = State.FREED;
      this.byteStream = null;
      notifyFreedResource();
    }
  }

  @Override
  public String toString() {
    return "MemoryFetchedInput [inputAttemptIdentifier="
        + inputAttemptIdentifier + ", actualSize=" + actualSize
        + ", compressedSize=" + compressedSize + ", type=" + type + ", id="
        + id + ", state=" + state + "]";
  }
}
