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
package org.apache.tez.runtime.library.common.sort.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.tez.runtime.library.common.Constants;

public class TezSpillRecord {
  public static final FsPermission SPILL_FILE_PERMS = new FsPermission((short) 0640);

  /** Backing store */
  private final ByteBuffer buf;
  /** View of backing storage as longs */
  private final LongBuffer entries;

  public TezSpillRecord(int numPartitions) {
    buf = ByteBuffer.allocate(
        numPartitions * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH);
    entries = buf.asLongBuffer();
  }

  public TezSpillRecord(Path indexFileName, Configuration job) throws IOException {
    this(indexFileName, job, null);
  }

  public TezSpillRecord(Path indexFileName, Configuration job, String expectedIndexOwner)
    throws IOException {
    this(indexFileName, job, new PureJavaCrc32(), expectedIndexOwner);
  }

  public TezSpillRecord(Path indexFileName, Configuration job, Checksum crc,
                     String expectedIndexOwner)
      throws IOException {

    final FileSystem rfs = FileSystem.getLocal(job).getRaw();
    final FSDataInputStream in = rfs.open(indexFileName);
    try {
      final long length = rfs.getFileStatus(indexFileName).getLen();
      final int partitions = 
          (int) length / Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH;
      final int size = partitions * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH;

      buf = ByteBuffer.allocate(size);
      if (crc != null) {
        crc.reset();
        CheckedInputStream chk = new CheckedInputStream(in, crc);
        IOUtils.readFully(chk, buf.array(), 0, size);
        if (chk.getChecksum().getValue() != in.readLong()) {
          throw new ChecksumException("Checksum error reading spill index: " +
                                indexFileName, -1);
        }
      } else {
        IOUtils.readFully(in, buf.array(), 0, size);
      }
      entries = buf.asLongBuffer();
    } finally {
      in.close();
    }
  }

  /**
   * Return number of IndexRecord entries in this spill.
   */
  public int size() {
    return entries.capacity() / (Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8);
  }

  /**
   * Get spill offsets for given partition.
   */
  public TezIndexRecord getIndex(int partition) {
    final int pos = partition * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
    return new TezIndexRecord(entries.get(pos), entries.get(pos + 1),
                           entries.get(pos + 2));
  }

  /**
   * Set spill offsets for given partition.
   */
  public void putIndex(TezIndexRecord rec, int partition) {
    final int pos = partition * Constants.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
    entries.put(pos, rec.getStartOffset());
    entries.put(pos + 1, rec.getRawLength());
    entries.put(pos + 2, rec.getPartLength());
  }

  /**
   * Write this spill record to the location provided.
   */
  public void writeToFile(Path loc, Configuration job)
      throws IOException {
    writeToFile(loc, job, new PureJavaCrc32());
  }

  public void writeToFile(Path loc, Configuration job, Checksum crc)
      throws IOException {
    final FileSystem rfs = FileSystem.getLocal(job).getRaw();
    CheckedOutputStream chk = null;
    final FSDataOutputStream out = rfs.create(loc);
    try {
      if (crc != null) {
        crc.reset();
        chk = new CheckedOutputStream(out, crc);
        chk.write(buf.array());
        out.writeLong(chk.getChecksum().getValue());
      } else {
        out.write(buf.array());
      }
    } finally {
      if (chk != null) {
        chk.close();
      } else {
        out.close();
      }
      if (!SPILL_FILE_PERMS.equals(SPILL_FILE_PERMS.applyUMask(FsPermission.getUMask(job)))) {
        rfs.setPermission(loc, SPILL_FILE_PERMS);
      }
    }
  }

}
