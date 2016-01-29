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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader;

/**
 * <code>IFile.InMemoryReader</code> to read map-outputs present in-memory.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class InMemoryReader extends Reader {

  private static class ByteArrayDataInput extends ByteArrayInputStream implements DataInput {

    public ByteArrayDataInput(byte buf[], int offset, int length) {
      super(buf, offset, length);
    }

    public void reset(byte[] input, int start, int length) {
      this.buf = input;
      this.count = start+length;
      this.mark = start;
      this.pos = start;
    }

    public byte[] getData() { return buf; }
    public int getPosition() { return pos; }
    public int getLength() { return count; }
    public int getMark() { return mark; }

    @Override
    public void readFully(byte[] b) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int skipBytes(int n) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean readBoolean() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte readByte() throws IOException {
      return (byte)read();
    }

    @Override
    public int readUnsignedByte() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public short readShort() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int readUnsignedShort() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public char readChar() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int readInt() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long readLong() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public float readFloat() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String readLine() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  private final InputAttemptIdentifier taskAttemptId;
  private final MergeManager merger;
  ByteArrayDataInput memDataIn;
  private int start;
  private int length;
  private int originalKeyPos;

  public InMemoryReader(MergeManager merger,
      InputAttemptIdentifier taskAttemptId, byte[] data, int start,
      int length)
      throws IOException {
    super(null, length - start, null, null, null, false, 0, -1);
    this.taskAttemptId = taskAttemptId;
    this.merger = merger;

    buffer = data;
    bufferSize = (int) length;
    memDataIn = new ByteArrayDataInput(buffer, start, length);
    this.start = start;
    this.length = length;
  }

  @Override
  public void reset(int offset) {
    memDataIn.reset(buffer, start + offset, length);
    bytesRead = offset;
    eof = false;
  }

  @Override
  public long getPosition() throws IOException {
    // InMemoryReader does not initialize streams like Reader, so in.getPos()
    // would not work. Instead, return the number of uncompressed bytes read,
    // which will be correct since in-memory data is not compressed.
    return bytesRead;
  }

  @Override
  public long getLength() {
    return length;
  }

  private void dumpOnError() {
    File dumpFile = new File("../output/" + taskAttemptId + ".dump");
    System.err.println("Dumping corrupt map-output of " + taskAttemptId +
                       " to " + dumpFile.getAbsolutePath());
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(dumpFile);
      fos.write(buffer, 0, bufferSize);
    } catch (IOException ioe) {
      System.err.println("Failed to dump map-output of " + taskAttemptId);
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          System.err.println("Failed to dump map-output of " + taskAttemptId);
        }
      }
    }
  }

  protected void readKeyValueLength(DataInput dIn) throws IOException {
    super.readKeyValueLength(dIn);
    if (currentKeyLength != IFile.RLE_MARKER) {
      originalKeyPos = memDataIn.getPosition();
    }
  }

  public KeyState readRawKey(DataInputBuffer key) throws IOException {
    try {
      if (!positionToNextRecord(memDataIn)) {
        return KeyState.NO_KEY;
      }
      // Setup the key
      int pos = memDataIn.getPosition();
      byte[] data = memDataIn.getData();
      if (currentKeyLength == IFile.RLE_MARKER) {
        // get key length from original key
        key.reset(data, originalKeyPos, originalKeyLength);
        return KeyState.SAME_KEY;
      }
      key.reset(data, pos, currentKeyLength);
      // Position for the next value
      long skipped = memDataIn.skip(currentKeyLength);
      if (skipped != currentKeyLength) {
        throw new IOException("Rec# " + recNo +
            ": Failed to skip past key of length: " +
            currentKeyLength);
      }
      bytesRead += currentKeyLength;
      return KeyState.NEW_KEY;
    } catch (IOException ioe) {
      dumpOnError();
      throw ioe;
    }
  }

  public void nextRawValue(DataInputBuffer value) throws IOException {
    try {
      int pos = memDataIn.getPosition();
      byte[] data = memDataIn.getData();
      value.reset(data, pos, currentValueLength);

      // Position for the next record
      long skipped = memDataIn.skip(currentValueLength);
      if (skipped != currentValueLength) {
        throw new IOException("Rec# " + recNo +
            ": Failed to skip past value of length: " +
            currentValueLength);
      }
      // Record the byte
      bytesRead += currentValueLength;
      ++recNo;
    } catch (IOException ioe) {
      dumpOnError();
      throw ioe;
    }
  }

  public void close() {
    // Release
    buffer = null;
    // Inform the MergeManager
    if (merger != null) {
      merger.unreserve(bufferSize);
    }
  }
}
