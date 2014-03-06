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
  private final InputAttemptIdentifier taskAttemptId;
  private final MergeManager merger;
  DataInputBuffer memDataIn = new DataInputBuffer();
  private int start;
  private int length;
  private int prevKeyPos;

  public InMemoryReader(MergeManager merger, InputAttemptIdentifier taskAttemptId,
                        byte[] data, int start, int length)
  throws IOException {
    super(null, length - start, null,null, null, false, 0, -1);
    this.merger = merger;
    this.taskAttemptId = taskAttemptId;

    buffer = data;
    bufferSize = (int)fileLength;
    memDataIn.reset(buffer, start, length);
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
    return fileLength;
  }
  
  private void dumpOnError() {
    File dumpFile = new File("../output/" + taskAttemptId + ".dump");
    System.err.println("Dumping corrupt map-output of " + taskAttemptId + 
                       " to " + dumpFile.getAbsolutePath());
    try {
      FileOutputStream fos = new FileOutputStream(dumpFile);
      fos.write(buffer, 0, bufferSize);
      fos.close();
    } catch (IOException ioe) {
      System.err.println("Failed to dump map-output of " + taskAttemptId);
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
      if(currentKeyLength == IFile.RLE_MARKER) {
        key.reset(data, prevKeyPos, prevKeyLength);
        currentKeyLength = prevKeyLength;
        return KeyState.SAME_KEY;
      }      
      key.reset(data, pos, currentKeyLength);
      prevKeyPos = pos;
      // Position for the next value
      long skipped = memDataIn.skip(currentKeyLength);
      if (skipped != currentKeyLength) {
        throw new IOException("Rec# " + recNo + 
            ": Failed to skip past key of length: " + 
            currentKeyLength);
      }

      // Record the byte
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
    dataIn = null;
    buffer = null;
      // Inform the MergeManager
    if (merger != null) {
      merger.unreserve(bufferSize);
    }
  }
}
