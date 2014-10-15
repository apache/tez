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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;

public class LocalDiskFetchedInput extends FetchedInput {
  private static final Log LOG = LogFactory.getLog(LocalDiskFetchedInput.class);

  private final Path inputFile;
  private final FileSystem localFS;
  private final long startOffset;

  public LocalDiskFetchedInput(long startOffset, long actualSize, long compressedSize,
                               InputAttemptIdentifier inputAttemptIdentifier, Path inputFile,
                               Configuration conf, FetchedInputCallback callbackHandler)
      throws IOException {
    super(Type.DISK_DIRECT, actualSize, compressedSize, inputAttemptIdentifier, callbackHandler);
    this.startOffset = startOffset;
    this.inputFile = inputFile;
    localFS = FileSystem.getLocal(conf);
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
      throw new IOException("Output Stream is not supported for " + this.toString());
  }

  @Override
  public InputStream getInputStream() throws IOException {
    FSDataInputStream inputStream = localFS.open(inputFile);
    inputStream.seek(startOffset);
    return new BoundedInputStream(inputStream, compressedSize);
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
      notifyFreedResource();
    }
  }

  @Override
  public String toString() {
    return "LocalDiskFetchedInput [inputFile path =" + inputFile +
        ", offset" + startOffset +
        ", actualSize=" + actualSize +
        ", compressedSize=" + compressedSize +
        ", inputAttemptIdentifier=" + inputAttemptIdentifier +
        ", type=" + type +
        ", id=" + id +
        ", state=" + state + "]";
  }

  @VisibleForTesting
  protected Path getInputFile() {
    return inputFile;
  }

  @VisibleForTesting
  protected long getStartOffset() {
    return startOffset;
  }

  @VisibleForTesting
  protected FileSystem getLocalFS() {
    return localFS;
  }

}
