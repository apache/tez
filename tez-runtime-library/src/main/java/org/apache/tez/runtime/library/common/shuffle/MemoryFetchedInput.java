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

  private byte[] byteArray;

  public MemoryFetchedInput(long actualSize,
      InputAttemptIdentifier inputAttemptIdentifier,
      FetchedInputCallback callbackHandler) {
    super(inputAttemptIdentifier, callbackHandler);
    this.byteArray = new byte[(int) actualSize];
  }

  @Override
  public Type getType() {
    return Type.MEMORY;
  }

  @Override
  public long getSize() {
    if (this.byteArray == null) {
      return 0;
    }
    return this.byteArray.length;
  }

  @Override
  public OutputStream getOutputStream() {
    return new InMemoryBoundedByteArrayOutputStream(byteArray);
  }

  @Override
  public InputStream getInputStream() {
    return new NonSyncByteArrayInputStream(byteArray);
  }

  public byte[] getBytes() {
    return byteArray;
  }
  
  @Override
  public void commit() {
    if (isState(State.PENDING)) {
      setState(State.COMMITTED);
      notifyFetchComplete();
    }
  }

  @Override
  public void abort() {
    if (isState(State.PENDING)) {
      setState(State.ABORTED);
      notifyFetchFailure();
    }
  }
  
  @Override
  public void free() {
    Preconditions.checkState(
        isState(State.COMMITTED) || isState(State.ABORTED),
        "FetchedInput can only be freed after it is committed or aborted");
    if (isState(State.COMMITTED)) { // ABORTED would have already called cleanup
      setState(State.FREED);
      notifyFreedResource();
      // Set this to null AFTER notifyFreedResource() so that getSize()
      // returns the correct size
      this.byteArray = null;
    }
  }

  @Override
  public String toString() {
    return "MemoryFetchedInput [inputAttemptIdentifier="
        + getInputAttemptIdentifier() + ", size=" + getSize()
        + ", type=" + getType() + ", id="
        + getId() + ", state=" + getState() + "]";
  }

  private static class InMemoryBoundedByteArrayOutputStream extends BoundedByteArrayOutputStream {
    InMemoryBoundedByteArrayOutputStream(byte[] array) {
      super(array, 0, array.length);
    }
  }
}
