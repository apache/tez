/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.common.shuffle;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

@Private
public abstract class FetchedInput {

  public static enum Type {
    WAIT, // TODO NEWTEZ Implement this, only if required.
    MEMORY,
    DISK,
    DISK_DIRECT
  }

  protected static enum State {
    PENDING, COMMITTED, ABORTED, FREED
  }

  private static AtomicInteger ID_GEN = new AtomicInteger(0);

  private InputAttemptIdentifier inputAttemptIdentifier;
  private final FetchedInputCallback callback;
  private final int id;
  private byte state;

  protected FetchedInput(InputAttemptIdentifier inputAttemptIdentifier,
                         FetchedInputCallback callbackHandler) {
    this.inputAttemptIdentifier = inputAttemptIdentifier;
    this.callback = callbackHandler;
    this.id = ID_GEN.getAndIncrement();
    this.state = (byte) State.PENDING.ordinal();
  }

  public abstract Type getType();

  protected boolean isState(State state) {
    return this.state == (byte) state.ordinal();
  }

  protected void setState(State state) {
    this.state = (byte) state.ordinal();
  }

  protected State getState() {
    if (isState(State.PENDING)) {
      return State.PENDING;
    }
    if (isState(State.COMMITTED)) {
      return State.COMMITTED;
    }
    if (isState(State.ABORTED)) {
      return State.ABORTED;
    }
    if (isState(State.FREED)) {
      return State.FREED;
    }
    // Should not get here
    return null;
  }

  protected int getId() {
    return this.id;
  }

  public abstract long getSize();

  public InputAttemptIdentifier getInputAttemptIdentifier() {
    return this.inputAttemptIdentifier;
  }

  /**
   * Inform the Allocator about a committed resource.
   * This should be called by commit
   */
  public void notifyFetchComplete() {
    this.callback.fetchComplete(this);
  }

  /**
   * Inform the Allocator about a failed resource.
   * This should be called by abort
   */
  public void notifyFetchFailure() {
    this.callback.fetchFailed(this);
  }

  /**
   * Inform the Allocator about a completed resource being released.
   * This should be called by free
   */
  public void notifyFreedResource() {
    this.callback.freeResources(this);
  }

  /**
   * Returns the output stream to be used to write fetched data. Users are
   * expected to close the OutputStream when they're done
   */
  public abstract OutputStream getOutputStream() throws IOException;

  /**
   * Return an input stream to be used to read the previously fetched data.
   * All calls to getInputStream() produce new reset streams for reading.
   * Users are expected to close the InputStream when they're done.
   */
  public abstract InputStream getInputStream() throws IOException;

  /**
   * Commit the output. Should be idempotent
   */
  public abstract void commit() throws IOException;

  /**
   * Abort the output. Should be idempotent
   */
  public abstract void abort() throws IOException;

  /**
   * Called when this input has been consumed, so that resources can be
   * reclaimed.
   */
  public abstract void free();

  @Override
  public int hashCode() {
    return id;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FetchedInput other = (FetchedInput) obj;
    if (id != other.id)
      return false;
    return true;
  }
}
