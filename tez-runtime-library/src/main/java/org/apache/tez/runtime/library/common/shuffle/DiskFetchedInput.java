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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;

import com.google.common.base.Preconditions;

public class DiskFetchedInput extends FetchedInput {

  private static final Logger LOG = LoggerFactory.getLogger(DiskFetchedInput.class);
  
  private final FileSystem localFS;
  private final Path tmpOutputPath;
  private final Path outputPath;
  private final long size;

  public DiskFetchedInput(long compressedSize,
      InputAttemptIdentifier inputAttemptIdentifier,
      FetchedInputCallback callbackHandler, Configuration conf,
      LocalDirAllocator localDirAllocator, TezTaskOutputFiles filenameAllocator)
      throws IOException {
    super(inputAttemptIdentifier, callbackHandler);

    this.size = compressedSize;
    this.localFS = FileSystem.getLocal(conf).getRaw();
    this.outputPath = filenameAllocator.getInputFileForWrite(
        this.getInputAttemptIdentifier().getInputIdentifier(), this
            .getInputAttemptIdentifier().getSpillEventId(), this.size);
    // Files are not clobbered due to the id being appended to the outputPath in the tmpPath,
    // otherwise fetches for the same task but from different attempts would clobber each other.
    this.tmpOutputPath = outputPath.suffix(String.valueOf(getId()));
  }

  @Override
  public Type getType() {
    return Type.DISK;
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return localFS.create(tmpOutputPath);
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return localFS.open(outputPath);
  }

  public final Path getInputPath() {
    if (isState(State.COMMITTED)) {
      return this.outputPath;
    }
    return this.tmpOutputPath;
  }
  
  @Override
  public void commit() throws IOException {
    if (isState(State.PENDING)) {
      setState(State.COMMITTED);
      localFS.rename(tmpOutputPath, outputPath);
      notifyFetchComplete();
    }
  }

  @Override
  public void abort() throws IOException {
    if (isState(State.PENDING)) {
      setState(State.ABORTED);
      // TODO NEWTEZ Maybe defer this to container cleanup
      localFS.delete(tmpOutputPath, false);
      notifyFetchFailure();
    }
  }
  
  @Override
  public void free() {
    Preconditions.checkState(
        isState(State.COMMITTED) || isState(State.ABORTED),
        "FetchedInput can only be freed after it is committed or aborted");
    if (isState(State.COMMITTED)) {
      setState(State.FREED);
      try {
        // TODO NEWTEZ Maybe defer this to container cleanup
        localFS.delete(outputPath, false);
      } catch (IOException e) {
        // Ignoring the exception, will eventually be cleaned by container
        // cleanup.
        LOG.warn("Failed to remvoe file : " + outputPath.toString());
      }
      notifyFreedResource();
    }
  }

  @Override
  public String toString() {
    return "DiskFetchedInput [outputPath=" + outputPath
        + ", inputAttemptIdentifier=" + getInputAttemptIdentifier()
        + ", actualSize=" + getSize()
        + ", type=" + getType() + ", id=" + getId() + ", state=" + getState() + "]";
  }
}
