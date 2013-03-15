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

package org.apache.tez.engine.common.sort.impl.dflt;

import java.io.IOException;
import java.io.InputStream;
import java.nio.IntBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.tez.engine.common.shuffle.impl.InMemoryWriter;
import org.apache.tez.engine.common.sort.impl.dflt.DefaultSorter.InMemValBytes;

class SortBufferInputStream extends InputStream {

  private static final Log LOG = LogFactory.getLog(SortBufferInputStream.class);
  
  private final InMemoryShuffleSorter sorter;
  private InMemoryWriter sortOutput;
  
  private int mend;
  private int recIndex;
  private final byte[] kvbuffer;       
  private final IntBuffer kvmeta;
  private final int partitionBytes;
  private final int partition;
  
  byte[] dualBuf = new byte[8192];
  DualBufferOutputStream out;
  private int readBytes = 0;
  
  public SortBufferInputStream(
      InMemoryShuffleSorter sorter, int partition) {
    this.sorter = sorter;
    this.partitionBytes = 
        (int)sorter.getShuffleHeader(partition).getCompressedLength();
    this.partition = partition;
    this.mend = sorter.getMetaEnd();
    this.recIndex = sorter.getSpillIndex(partition);
    this.kvbuffer = sorter.kvbuffer;
    this.kvmeta = sorter.kvmeta;
    out = new DualBufferOutputStream(null, 0, 0, dualBuf);
    sortOutput = new InMemoryWriter(out);
  }
  
  byte[] one = new byte[1];
  
  @Override
  public int read() throws IOException {
    int b = read(one, 0, 1);
    return (b == -1) ? b : one[0]; 
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (available() == 0) {
      return -1;
    }
    
    int currentOffset = off;
    int currentLength = len;
    int currentReadBytes = 0;
    
    // Check if there is residual data in the dualBuf
    int residualLen = out.getCurrent();
    if (residualLen > 0) {
      int readable = Math.min(currentLength, residualLen);
      System.arraycopy(dualBuf, 0, b, currentOffset, readable);
      currentOffset += readable;
      currentReadBytes += readable;
      out.setCurrentPointer(-readable);
      
      // buffer has less capacity
      currentLength -= readable;
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("XXX read_residual:" +
            " readable=" + readable +
            " readBytes=" + readBytes);
      }
    }
    
    // Now, use the provided buffer
    if (LOG.isDebugEnabled()) {
      LOG.debug("XXX read: out.reset" +
          " b=" + b + 
          " currentOffset=" + currentOffset + 
          " currentLength=" + currentLength +
          " recIndex=" + recIndex);
    }
    out.reset(b, currentOffset, currentLength);
    
    // Read from sort-buffer into the provided buffer, space permitting
    DataInputBuffer key = new DataInputBuffer();
    final InMemValBytes value = sorter.createInMemValBytes();
    
    int kvPartition = 0;
    int numRec = 0;
    for (;
         currentLength > 0 && recIndex < mend && 
             (kvPartition = getKVPartition(recIndex)) == partition;
        ++recIndex) {
      
      final int kvoff = sorter.offsetFor(recIndex);
      
      int keyLen = 
          (kvmeta.get(kvoff + InMemoryShuffleSorter.VALSTART) - 
              kvmeta.get(kvoff + InMemoryShuffleSorter.KEYSTART));
      key.reset(
          kvbuffer, 
          kvmeta.get(kvoff + InMemoryShuffleSorter.KEYSTART),
          keyLen
          );
      
      int valLen = sorter.getVBytesForOffset(kvoff, value);

      int recLen = 
          (keyLen + WritableUtils.getVIntSize(keyLen)) + 
          (valLen + WritableUtils.getVIntSize(valLen));
      
      currentReadBytes += recLen;
      currentOffset += recLen;
      currentLength -= recLen;

      // Write out key/value into the in-mem ifile
      if (LOG.isDebugEnabled()) {
        LOG.debug("XXX read: sortOutput.append" +
            " #rec=" + ++numRec +
            " recIndex=" + recIndex + " kvoff=" + kvoff + 
            " keyLen=" + keyLen + " valLen=" + valLen + " recLen=" + recLen +
            " readBytes=" + readBytes +
            " currentReadBytes="  + currentReadBytes +
            " currentLength=" + currentLength);
      }
      sortOutput.append(key, value);
    }

    // If we are at the end of the segment, close the ifile
    if (currentLength > 0 && 
        (recIndex == mend || kvPartition != partition)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("XXX About to call close:" +
            " currentLength=" + currentLength + 
            " recIndex=" + recIndex + " mend=" + mend + 
            " kvPartition=" + kvPartition + " partitino=" + partition);
      }
      sortOutput.close();
      currentReadBytes += 
          (InMemoryShuffleSorter.IFILE_EOF_LENGTH + 
              InMemoryShuffleSorter.IFILE_CHECKSUM_LENGTH);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("XXX Hmm..." +
            " currentLength=" + currentLength + 
            " recIndex=" + recIndex + " mend=" + mend + 
            " kvPartition=" + kvPartition + " partitino=" + partition);
      }
    }
    
    int retVal = Math.min(currentReadBytes, len);
    readBytes += retVal;
    if (LOG.isDebugEnabled()) {
      LOG.debug("XXX read: done" +
          " retVal=" + retVal + 
          " currentReadBytes=" + currentReadBytes +
          " len=" + len + 
          " readBytes=" + readBytes +
          " partitionBytes=" + partitionBytes +
          " residualBytes=" + out.getCurrent());
    }
    return retVal;
  }

  private int getKVPartition(int recIndex) {
    return kvmeta.get(
        sorter.offsetFor(recIndex) + InMemoryShuffleSorter.PARTITION);
  }
  
  @Override
  public int available() throws IOException {
    return (partitionBytes - readBytes);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  @Override
  public boolean markSupported() {
    return false;
  }
  
  static class DualBufferOutputStream extends BoundedByteArrayOutputStream {

    byte[] dualBuf;
    int currentPointer = 0;
    byte[] one = new byte[1];
    
    public DualBufferOutputStream(
        byte[] buf, int offset, int length, 
        byte[] altBuf) {
      super(buf, offset, length);
      this.dualBuf = altBuf;
    }
    
    public void reset(byte[] b, int off, int len) {
      super.resetBuffer(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
      one[0] = (byte)b;
      write(one, 0, 1);
    }

    @Override
    public void write(byte[] b) throws IOException {
      write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      int available = super.available();
      if (available >= len) {
        super.write(b, off, len);
      } else {
        super.write(b, off, available);
        System.arraycopy(b, off+available, dualBuf, currentPointer, len-available);
        currentPointer += (len - available);
      }
    }
    
    int getCurrent() {
      return currentPointer;
    }
    
    void setCurrentPointer(int delta) {
      if ((currentPointer + delta) > dualBuf.length) {
        throw new IndexOutOfBoundsException("Trying to set dualBuf 'current'" +
        		" marker to " + (currentPointer+delta) + " when " +
        		" dualBuf.length is " + dualBuf.length);
      }
      currentPointer = (currentPointer + delta) % dualBuf.length;
    }
  }
}
