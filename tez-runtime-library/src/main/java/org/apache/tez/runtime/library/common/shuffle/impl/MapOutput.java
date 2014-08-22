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
package org.apache.tez.runtime.library.common.shuffle.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.FileChunk;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;


class MapOutput {
  private static final Log LOG = LogFactory.getLog(MapOutput.class);
  private static AtomicInteger ID = new AtomicInteger(0);
  
  public static enum Type {
    WAIT,
    MEMORY,
    DISK,
    DISK_DIRECT
  }

  private final int id;
  private final Type type;
  private InputAttemptIdentifier attemptIdentifier;
  private final long size;

  private final boolean primaryMapOutput;
  private final MergeManager merger;

  // MEMORY
  private final byte[] memory;
  private BoundedByteArrayOutputStream byteStream;

  // DISK
  private final FileSystem localFS;
  private final Path tmpOutputPath;
  private final FileChunk outputPath;
  private OutputStream disk;

  private MapOutput(Type type, InputAttemptIdentifier attemptIdentifier, MergeManager merger,
                    long size, Path outputPath, long offset, boolean primaryMapOutput,
                    FileSystem fs, Path tmpOutputPath) {
    this.id = ID.incrementAndGet();
    this.type = type;
    this.attemptIdentifier = attemptIdentifier;
    this.merger = merger;
    this.primaryMapOutput = primaryMapOutput;

    this.localFS = fs;
    this.size = size;

    // Other type specific values

    if (type == Type.MEMORY) {
      // since we are passing an int from createMemoryMapOutput, its safe to cast to int
      this.byteStream = new BoundedByteArrayOutputStream((int)size);
      this.memory = byteStream.getBuffer();
    } else {
      this.byteStream = null;
      this.memory = null;
    }

    this.tmpOutputPath = tmpOutputPath;
    this.disk = null;

    if (type == Type.DISK || type == Type.DISK_DIRECT) {
      boolean preserve = (type == Type.DISK_DIRECT); // type disk are temp files.
      this.outputPath = new FileChunk(outputPath, offset, size, preserve);
    } else {
      this.outputPath = null;
    }

  }

  public static MapOutput createDiskMapOutput(InputAttemptIdentifier attemptIdentifier,
                                              MergeManager merger, long size, Configuration conf,
                                              int fetcher, boolean primaryMapOutput,
                                              TezTaskOutputFiles mapOutputFile) throws
      IOException {
    FileSystem fs = FileSystem.getLocal(conf);
    Path outputpath = mapOutputFile.getInputFileForWrite(
        attemptIdentifier.getInputIdentifier().getInputIndex(), size);
    Path tmpOuputPath = outputpath.suffix(String.valueOf(fetcher));
    long offset = 0;

    MapOutput mapOutput = new MapOutput(Type.DISK, attemptIdentifier, merger, size, outputpath, offset,
        primaryMapOutput, fs, tmpOuputPath);
    mapOutput.disk = mapOutput.localFS.create(tmpOuputPath);

    return mapOutput;
  }

  public static MapOutput createLocalDiskMapOutput(InputAttemptIdentifier attemptIdentifier,
                                                   MergeManager merger, Path path,  long offset,
                                                   long size, boolean primaryMapOutput)  {
    return new MapOutput(Type.DISK_DIRECT, attemptIdentifier, merger, size, path, offset,
        primaryMapOutput, null, null);
  }

  public static MapOutput createMemoryMapOutput(InputAttemptIdentifier attemptIdentifier,
                                                MergeManager merger, int size,
                                                boolean primaryMapOutput)  {
    return new MapOutput(Type.MEMORY, attemptIdentifier, merger, size, null, -1, primaryMapOutput,
        null, null);
  }

  public static MapOutput createWaitMapOutput(InputAttemptIdentifier attemptIdentifier) {
    return new MapOutput(Type.WAIT, attemptIdentifier, null, -1, null, -1, false, null, null);
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
    return outputPath;
  }

  public byte[] getMemory() {
    return memory;
  }

  public BoundedByteArrayOutputStream getArrayStream() {
    return byteStream;
  }
  
  public OutputStream getDisk() {
    return disk;
  }

  public InputAttemptIdentifier getAttemptIdentifier() {
    return this.attemptIdentifier;
  }

  public Type getType() {
    return type;
  }

  public long getSize() {
    return size;
  }

  public void commit() throws IOException {
    if (type == Type.MEMORY) {
      merger.closeInMemoryFile(this);
    } else if (type == Type.DISK) {
      localFS.rename(tmpOutputPath, outputPath.getPath());
      merger.closeOnDiskFile(outputPath);
    } else if (type == Type.DISK_DIRECT) {
      merger.closeOnDiskFile(outputPath);
    } else {
      throw new IOException("Cannot commit MapOutput of type WAIT!");
    }
  }
  
  public void abort() {
    if (type == Type.MEMORY) {
      merger.unreserve(memory.length);
    } else if (type == Type.DISK) {
      try {
        localFS.delete(tmpOutputPath, false);
      } catch (IOException ie) {
        LOG.info("failure to clean up " + tmpOutputPath, ie);
      }
    } else if (type == Type.DISK_DIRECT) { //nothing to do.
    } else {
      throw new IllegalArgumentException
                   ("Cannot commit MapOutput with of type WAIT!");
    }
  }
  
  public String toString() {
    return "MapOutput( AttemptIdentifier: " + attemptIdentifier + ", Type: " + type + ")";
  }
  
  public static class MapOutputComparator 
  implements Comparator<MapOutput> {
    public int compare(MapOutput o1, MapOutput o2) {
      if (o1.id == o2.id) { 
        return 0;
      }
      
      if (o1.size < o2.size) {
        return -1;
      } else if (o1.size > o2.size) {
        return 1;
      }
      
      if (o1.id < o2.id) {
        return -1;
      } else {
        return 1;
      }
    }
  }
}
