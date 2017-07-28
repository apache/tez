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
package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.FileChunk;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;


abstract class MapOutput {
  private static final Logger LOG = LoggerFactory.getLogger(MapOutput.class);
  private static AtomicInteger ID = new AtomicInteger(0);
  
  public enum Type {
    WAIT,
    MEMORY,
    DISK,
    DISK_DIRECT
  }

  private final int id;
  private InputAttemptIdentifier attemptIdentifier;

  private final boolean primaryMapOutput;
  protected final FetchedInputAllocatorOrderedGrouped callback;

  private MapOutput(InputAttemptIdentifier attemptIdentifier, FetchedInputAllocatorOrderedGrouped callback,
                    boolean primaryMapOutput) {
    this.id = ID.incrementAndGet();
    this.attemptIdentifier = attemptIdentifier;
    this.callback = callback;
    this.primaryMapOutput = primaryMapOutput;
  }

  public static MapOutput createDiskMapOutput(InputAttemptIdentifier attemptIdentifier,
                                              FetchedInputAllocatorOrderedGrouped callback, long size, Configuration conf,
                                              int fetcher, boolean primaryMapOutput,
                                              TezTaskOutputFiles mapOutputFile) throws
      IOException {
    FileSystem fs = FileSystem.getLocal(conf).getRaw();
    Path outputPath = mapOutputFile.getInputFileForWrite(
        attemptIdentifier.getInputIdentifier(), attemptIdentifier.getSpillEventId(), size);
    // Files are not clobbered due to the id being appended to the outputPath in the tmpPath,
    // otherwise fetches for the same task but from different attempts would clobber each other.
    Path tmpOutputPath = outputPath.suffix(String.valueOf(fetcher));
    long offset = 0;

    DiskMapOutput mapOutput = new DiskMapOutput(attemptIdentifier, callback, size, outputPath, offset,
        primaryMapOutput, tmpOutputPath);
    mapOutput.disk = fs.create(tmpOutputPath);

    return mapOutput;
  }

  public static MapOutput createLocalDiskMapOutput(InputAttemptIdentifier attemptIdentifier,
                                                   FetchedInputAllocatorOrderedGrouped callback, Path path,  long offset,
                                                   long size, boolean primaryMapOutput)  {
    return new DiskDirectMapOutput(attemptIdentifier, callback, size, path, offset,
        primaryMapOutput);
  }

  public static MapOutput createMemoryMapOutput(InputAttemptIdentifier attemptIdentifier,
                                                FetchedInputAllocatorOrderedGrouped callback, int size,
                                                boolean primaryMapOutput)  {
    return new InMemoryMapOutput(attemptIdentifier, callback, size, primaryMapOutput);
  }

  public static MapOutput createWaitMapOutput(InputAttemptIdentifier attemptIdentifier) {
    return new WaitMapOutput(attemptIdentifier);
  }

  public boolean isPrimaryMapOutput() {
    return primaryMapOutput;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MapOutput) {
      return id == ((MapOutput)obj).id;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id;
  }

  public FileChunk getOutputPath() {
    return null;
  }

  public byte[] getMemory() {
    return null;
  }
  
  public OutputStream getDisk() {
    return null;
  }

  public InputAttemptIdentifier getAttemptIdentifier() {
    return this.attemptIdentifier;
  }

  public abstract Type getType();

  public long getSize() {
    return -1;
  }

  public void commit() throws IOException {
  }
  
  public void abort() {
  }
  
  public String toString() {
    return "MapOutput( AttemptIdentifier: " + attemptIdentifier + ", Type: " + getType() + ")";
  }
  
  public static class MapOutputComparator 
  implements Comparator<MapOutput> {
    public int compare(MapOutput o1, MapOutput o2) {
      if (o1.id == o2.id) { 
        return 0;
      }
      
      if (o1.getSize() < o2.getSize()) {
        return -1;
      } else if (o1.getSize() > o2.getSize()) {
        return 1;
      }
      
      if (o1.id < o2.id) {
        return -1;
      } else {
        return 1;
      }
    }
  }

  private static class DiskDirectMapOutput extends MapOutput {
    private final FileChunk outputPath;
    private DiskDirectMapOutput(InputAttemptIdentifier attemptIdentifier, FetchedInputAllocatorOrderedGrouped callback,
                      long size, Path outputPath, long offset, boolean primaryMapOutput) {
      super(attemptIdentifier, callback, primaryMapOutput);
      this.outputPath = new FileChunk(outputPath, offset, size, true, attemptIdentifier);
    }

    @Override
    public FileChunk getOutputPath() {
      return outputPath;
    }

    @Override
    public long getSize() {
      return outputPath.getLength();
    }

    @Override
    public void commit() throws IOException {
      callback.closeOnDiskFile(outputPath);
    }

    @Override
    public void abort() {
      // nothing to do
    }

    @Override
    public Type getType() {
      return Type.DISK_DIRECT;
    }
  }

  private static class DiskMapOutput extends MapOutput {
    private final Path tmpOutputPath;
    private final FileChunk outputPath;
    private OutputStream disk;
    private DiskMapOutput(InputAttemptIdentifier attemptIdentifier, FetchedInputAllocatorOrderedGrouped callback,
                                long size, Path outputPath, long offset, boolean primaryMapOutput, Path tmpOutputPath) {
      super(attemptIdentifier, callback, primaryMapOutput);

      this.tmpOutputPath = tmpOutputPath;
      this.disk = null;
      this.outputPath = new FileChunk(outputPath, offset, size, false, attemptIdentifier);
    }

    @Override
    public FileChunk getOutputPath() {
      return outputPath;
    }

    @Override
    public OutputStream getDisk() {
      return disk;
    }

    @Override
    public long getSize() {
      return outputPath.getLength();
    }

    @Override
    public void commit() throws IOException {
      callback.getLocalFileSystem().rename(tmpOutputPath, outputPath.getPath());
      callback.closeOnDiskFile(outputPath);
    }

    @Override
    public void abort() {
      try {
        callback.getLocalFileSystem().delete(tmpOutputPath, true);
      } catch (IOException ie) {
        LOG.info("failure to clean up " + tmpOutputPath, ie);
      }
    }

    @Override
    public Type getType() {
      return Type.DISK;
    }
  }

  private static class InMemoryMapOutput extends MapOutput {
    private byte[] byteArray;
    private InMemoryMapOutput(InputAttemptIdentifier attemptIdentifier,
                              FetchedInputAllocatorOrderedGrouped callback,
                              long size, boolean primaryMapOutput) {
      super(attemptIdentifier, callback, primaryMapOutput);
      this.byteArray = new byte[(int)size];
    }

    @Override
    public byte[] getMemory() {
      return byteArray;
    }

    @Override
    public long getSize() {
      return byteArray.length;
    }

    @Override
    public void commit() throws IOException {
      callback.closeInMemoryFile(this);
    }

    @Override
    public void abort() {
      callback.unreserve(byteArray.length);
    }

    @Override
    public Type getType() {
      return Type.MEMORY;
    }
  }

  private static class WaitMapOutput extends MapOutput {
    private WaitMapOutput(InputAttemptIdentifier attemptIdentifier) {
      super(attemptIdentifier, null, false);
    }

    @Override
    public void commit() throws IOException {
      throw new IOException("Cannot commit MapOutput of type WAIT!");
    }

    @Override
    public void abort() {
      throw new IllegalArgumentException("Cannot commit MapOutput of type WAIT!");
    }

    @Override
    public Type getType() {
      return Type.WAIT;
    }
  }
}
