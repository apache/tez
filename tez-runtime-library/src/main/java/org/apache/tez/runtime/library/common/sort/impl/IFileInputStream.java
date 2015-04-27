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

import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.HasFileDescriptor;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.util.DataChecksum;
/**
 * A checksum input stream, used for IFiles.
 * Used to validate the checksum of files created by {@link IFileOutputStream}. 
*/
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class IFileInputStream extends InputStream {
  
  private final InputStream in; //The input stream to be verified for checksum.
  private final FileDescriptor inFd; // the file descriptor, if it is known
  private final long length; //The total length of the input file
  private final long dataLength;
  private DataChecksum sum;
  private long currentOffset = 0;
  private final byte b[] = new byte[1];
  private byte csum[] = null;
  private int checksumSize;
  private byte[] buffer;
  private int offset;

  private ReadaheadRequest curReadahead = null;
  private ReadaheadPool raPool = ReadaheadPool.getInstance();
  private final boolean readahead;
  private final int readaheadLength;

  public static final Logger LOG = LoggerFactory.getLogger(IFileInputStream.class);

  private boolean disableChecksumValidation = false;

  /**
   * Create a checksum input stream that reads without readAhead.
   * @param in
   * @param len
   */
  public IFileInputStream(InputStream in, long len) {
    this(in, len, false, 0);
  }

  /**
   * Create a checksum input stream that reads
   * @param in The input stream to be verified for checksum.
   * @param len The length of the input stream including checksum bytes.
   * @param readAhead Whether to attempt readAhead for this stream
   * @param readAheadLength Number of bytes to readAhead if it is enabled
   */
  public IFileInputStream(InputStream in, long len, boolean readAhead, int readAheadLength) {
    this.in = in;
    sum = DataChecksum.newDataChecksum(DataChecksum.Type.CRC32,
        Integer.MAX_VALUE);
    checksumSize = sum.getChecksumSize();
    buffer = new byte[4096];
    offset = 0;
    length = len;
    dataLength = length - checksumSize;

    readahead = readAhead;
    readaheadLength = readAheadLength;

    if (readahead) {
      this.inFd = getFileDescriptorIfAvail(in);
      doReadahead();
    } else {
      this.inFd = null;
    }
  }

  private static FileDescriptor getFileDescriptorIfAvail(InputStream in) {
    FileDescriptor fd = null;
    try {
      if (in instanceof HasFileDescriptor) {
        fd = ((HasFileDescriptor)in).getFileDescriptor();
      } else if (in instanceof FileInputStream) {
        fd = ((FileInputStream)in).getFD();
      }
    } catch (IOException e) {
      LOG.info("Unable to determine FileDescriptor", e);
    }
    return fd;
  }

  /**
   * Close the input stream. Note that we need to read to the end of the
   * stream to validate the checksum.
   */
  @Override
  public void close() throws IOException {

    if (curReadahead != null) {
      curReadahead.cancel();
    }
    if (currentOffset < dataLength) {
      byte[] t = new byte[Math.min((int)
            (Integer.MAX_VALUE & (dataLength - currentOffset)), 32 * 1024)];
      while (currentOffset < dataLength) {
        int n = read(t, 0, t.length);
        if (0 == n) {
          throw new EOFException("Could not validate checksum");
        }
      }
    }
    in.close();
  }
  
  @Override
  public long skip(long n) throws IOException {
   throw new IOException("Skip not supported for IFileInputStream");
  }
  
  public long getPosition() {
    return (currentOffset >= dataLength) ? dataLength : currentOffset;
  }
  
  public long getSize() {
    return checksumSize;
  }

  private void checksum(byte[] b, int off, int len) {
    if(len >= buffer.length) {
      sum.update(buffer, 0, offset);
      offset = 0;
      sum.update(b, off, len);
      return;
    }
    final int remaining = buffer.length - offset;
    if(len > remaining) {
      sum.update(buffer, 0, offset);
      offset = 0;
    }
    /* now we should have len < buffer.length */
    System.arraycopy(b, off, buffer, offset, len);
    offset += len;
  }
  
  /**
   * Read bytes from the stream.
   * At EOF, checksum is validated, but the checksum
   * bytes are not passed back in the buffer. 
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {

    if (currentOffset >= dataLength) {
      return -1;
    }

    doReadahead();

    return doRead(b,off,len);
  }

  private void doReadahead() {
    if (raPool != null && inFd != null && readahead) {
      curReadahead = raPool.readaheadStream(
          "ifile", inFd,
          currentOffset, readaheadLength, dataLength,
          curReadahead);
    }
  }

  /**
   * Read bytes from the stream.
   * At EOF, checksum is validated and sent back
   * as the last four bytes of the buffer. The caller should handle
   * these bytes appropriately
   */
  public int readWithChecksum(byte[] b, int off, int len) throws IOException {

    if (currentOffset == length) {
      return -1;
    }
    else if (currentOffset >= dataLength) {
      // If the previous read drained off all the data, then just return
      // the checksum now. Note that checksum validation would have 
      // happened in the earlier read
      int lenToCopy = (int) (checksumSize - (currentOffset - dataLength));
      if (len < lenToCopy) {
        lenToCopy = len;
      }
      System.arraycopy(csum, (int) (currentOffset - dataLength), b, off, 
          lenToCopy);
      currentOffset += lenToCopy;
      return lenToCopy;
    }

    int bytesRead = doRead(b,off,len);

    if (currentOffset == dataLength) {
      if (len >= bytesRead + checksumSize) {
        System.arraycopy(csum, 0, b, off + bytesRead, checksumSize);
        bytesRead += checksumSize;
        currentOffset += checksumSize;
      }
    }
    return bytesRead;
  }

  private int doRead(byte[]b, int off, int len) throws IOException {
    
    // If we are trying to read past the end of data, just read
    // the left over data
    int origLen = len;
    if (currentOffset + len > dataLength) {
      len = (int) (dataLength - currentOffset);
    }
    
    int bytesRead = in.read(b, off, len);

    if (bytesRead < 0) {
      String mesg = " CurrentOffset=" + currentOffset +
          ", offset=" + offset +
          ", off=" + off +
          ", dataLength=" + dataLength + 
          ", origLen=" + origLen +
          ", len=" + len +
          ", length=" + length +
          ", checksumSize=" + checksumSize;
      LOG.info(mesg);
      throw new ChecksumException("Checksum Error: " + mesg, 0);
    }

    checksum(b, off, bytesRead);

    currentOffset += bytesRead;

    if (disableChecksumValidation) {
      return bytesRead;
    }
    
    if (currentOffset == dataLength) {
      //TODO: add checksumSize to currentOffset.
      // The last four bytes are checksum. Strip them and verify
      sum.update(buffer, 0, offset);
      csum = new byte[checksumSize];
      IOUtils.readFully(in, csum, 0, checksumSize);
      if (!sum.compare(csum, 0)) {
        String mesg = "CurrentOffset=" + currentOffset +
            ", off=" + offset +
            ", dataLength=" + dataLength + 
            ", origLen=" + origLen +
            ", len=" + len +
            ", length=" + length +
            ", checksumSize=" + checksumSize+
            ", csum=" + Arrays.toString(csum) +
            ", sum=" + sum; 
        LOG.info(mesg);

        throw new ChecksumException("Checksum Error: " + mesg, 0);
      }
    }
    return bytesRead;
  }


  @Override
  public int read() throws IOException {    
    b[0] = 0;
    int l = read(b,0,1);
    if (l < 0)  return l;
    
    // Upgrade the b[0] to an int so as not to misinterpret the
    // first bit of the byte as a sign bit
    int result = 0xFF & b[0];
    return result;
  }

  void disableChecksumValidation() {
    disableChecksumValidation = true;
  }
}
