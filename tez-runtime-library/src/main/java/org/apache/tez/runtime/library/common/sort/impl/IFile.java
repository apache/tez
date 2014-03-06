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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.BufferUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.tez.common.counters.TezCounter;

/**
 * <code>IFile</code> is the simple <key-len, value-len, key, value> format
 * for the intermediate map-outputs in Map-Reduce.
 *
 * There is a <code>Writer</code> to write out map-outputs in this format and 
 * a <code>Reader</code> to read files of this format.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class IFile {
  private static final Log LOG = LogFactory.getLog(IFile.class);
  public static final int EOF_MARKER = -1; // End of File Marker
  public static final int RLE_MARKER = -2; // Repeat same key marker
  public static final DataInputBuffer REPEAT_KEY = new DataInputBuffer();
    
  /**
   * <code>IFile.Writer</code> to write out intermediate map-outputs. 
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static class Writer {
    FSDataOutputStream out;
    boolean ownOutputStream = false;
    long start = 0;
    FSDataOutputStream rawOut;
    AtomicBoolean closed = new AtomicBoolean(false);
    
    CompressionOutputStream compressedOut;
    Compressor compressor;
    boolean compressOutput = false;
    
    long decompressedBytesWritten = 0;
    long compressedBytesWritten = 0;

    // Count records written to disk
    private long numRecordsWritten = 0;
    private final TezCounter writtenRecordsCounter;
    private final TezCounter serializedUncompressedBytes;

    IFileOutputStream checksumOut;

    Class keyClass;
    Class valueClass;
    Serializer keySerializer;
    Serializer valueSerializer;
    
    DataOutputBuffer buffer = new DataOutputBuffer();
    DataOutputBuffer previous = new DataOutputBuffer();
    
    // de-dup keys or not
    private boolean rle = false;

    public Writer(Configuration conf, FileSystem fs, Path file, 
                  Class keyClass, Class valueClass,
                  CompressionCodec codec,
                  TezCounter writesCounter,
                  TezCounter serializedBytesCounter) throws IOException {
      this(conf, fs.create(file), keyClass, valueClass, codec,
           writesCounter, serializedBytesCounter);
      ownOutputStream = true;
    }
    
    protected Writer(TezCounter writesCounter, TezCounter serializedBytesCounter) {
      writtenRecordsCounter = writesCounter;
      serializedUncompressedBytes = serializedBytesCounter;
    }

    public Writer(Configuration conf, FSDataOutputStream out, 
        Class keyClass, Class valueClass,
        CompressionCodec codec, TezCounter writesCounter, TezCounter serializedBytesCounter)
        throws IOException {
      this.writtenRecordsCounter = writesCounter;
      this.serializedUncompressedBytes = serializedBytesCounter;
      this.checksumOut = new IFileOutputStream(out);
      this.rawOut = out;
      this.start = this.rawOut.getPos();
      if (codec != null) {
        this.compressor = CodecPool.getCompressor(codec);
        if (this.compressor != null) {
          this.compressor.reset();
          this.compressedOut = codec.createOutputStream(checksumOut, compressor);
          this.out = new FSDataOutputStream(this.compressedOut,  null);
          this.compressOutput = true;
        } else {
          LOG.warn("Could not obtain compressor from CodecPool");
          this.out = new FSDataOutputStream(checksumOut,null);
        }
      } else {
        this.out = new FSDataOutputStream(checksumOut,null);
      }
      
      this.keyClass = keyClass;
      this.valueClass = valueClass;

      if (keyClass != null) {
        SerializationFactory serializationFactory = 
          new SerializationFactory(conf);
        this.keySerializer = serializationFactory.getSerializer(keyClass);
        this.keySerializer.open(buffer);
        this.valueSerializer = serializationFactory.getSerializer(valueClass);
        this.valueSerializer.open(buffer);
      }
    }

    public Writer(Configuration conf, FileSystem fs, Path file) 
    throws IOException {
      this(conf, fs, file, null, null, null, null, null);
    }

    public void close() throws IOException {
      if (closed.getAndSet(true)) {
        throw new IOException("Writer was already closed earlier");
      }

      // When IFile writer is created by BackupStore, we do not have
      // Key and Value classes set. So, check before closing the
      // serializers
      if (keyClass != null) {
        keySerializer.close();
        valueSerializer.close();
      }

      // Write EOF_MARKER for key/value length
      WritableUtils.writeVInt(out, EOF_MARKER);
      WritableUtils.writeVInt(out, EOF_MARKER);
      decompressedBytesWritten += 2 * WritableUtils.getVIntSize(EOF_MARKER);
      
      //Flush the stream
      out.flush();
  
      if (compressOutput) {
        // Flush
        compressedOut.finish();
        compressedOut.resetState();
      }
      
      // Close the underlying stream iff we own it...
      if (ownOutputStream) {
        out.close();
      }
      else {
        // Write the checksum
        checksumOut.finish();
      }

      compressedBytesWritten = rawOut.getPos() - start;

      if (compressOutput) {
        // Return back the compressor
        CodecPool.returnCompressor(compressor);
        compressor = null;
      }

      out = null;
      if(writtenRecordsCounter != null) {
        writtenRecordsCounter.increment(numRecordsWritten);
      }
    }

    public void append(Object key, Object value) throws IOException {
      if (key.getClass() != keyClass)
        throw new IOException("wrong key class: "+ key.getClass()
                              +" is not "+ keyClass);
      if (value.getClass() != valueClass)
        throw new IOException("wrong value class: "+ value.getClass()
                              +" is not "+ valueClass);
      
      boolean sameKey = false;

      // Append the 'key'
      keySerializer.serialize(key);
      int keyLength = buffer.getLength();
      if (keyLength < 0) {
        throw new IOException("Negative key-length not allowed: " + keyLength + 
                              " for " + key);
      }     
      
      if(rle && keyLength == previous.getLength()) {
        sameKey = (BufferUtils.compare(previous, buffer) == 0);       
      }
      
      if(!sameKey) {
        BufferUtils.copy(buffer, previous);
      }

      // Append the 'value'
      valueSerializer.serialize(value);
      int valueLength = buffer.getLength() - keyLength;
      if (valueLength < 0) {
        throw new IOException("Negative value-length not allowed: " + 
                              valueLength + " for " + value);
      }
      
      if(rle && sameKey) {
        WritableUtils.writeVInt(out, RLE_MARKER);                   // Same key as previous
        WritableUtils.writeVInt(out, valueLength);                  // value length
        out.write(buffer.getData(), keyLength, buffer.getLength()); // only the value
        // Update bytes written
        decompressedBytesWritten += 0 + valueLength + 
                                    WritableUtils.getVIntSize(RLE_MARKER) + 
                                    WritableUtils.getVIntSize(valueLength);
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(0 + valueLength);
        }
      } else {        
        // Write the record out        
        WritableUtils.writeVInt(out, keyLength);                  // key length
        WritableUtils.writeVInt(out, valueLength);                // value length
        out.write(buffer.getData(), 0, buffer.getLength());       // data
        // Update bytes written
        decompressedBytesWritten += keyLength + valueLength + 
                                    WritableUtils.getVIntSize(keyLength) + 
                                    WritableUtils.getVIntSize(valueLength);
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(keyLength + valueLength);
        }
      }

      // Reset
      buffer.reset();
      
      
      ++numRecordsWritten;
    }
    
    public void append(DataInputBuffer key, DataInputBuffer value)
    throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      if (keyLength < 0) {
        throw new IOException("Negative key-length not allowed: " + keyLength + 
                              " for " + key);
      }
      
      int valueLength = value.getLength() - value.getPosition();
      if (valueLength < 0) {
        throw new IOException("Negative value-length not allowed: " + 
                              valueLength + " for " + value);
      }
      
      boolean sameKey = false;
      
      if(rle && keyLength == previous.getLength()) {
        sameKey = (keyLength != 0) && (BufferUtils.compare(previous, key) == 0);        
      }
      
      if(rle && sameKey) {
        WritableUtils.writeVInt(out, RLE_MARKER);
        WritableUtils.writeVInt(out, valueLength);        
        out.write(value.getData(), value.getPosition(), valueLength);

        // Update bytes written
        decompressedBytesWritten += 0 + valueLength
            + WritableUtils.getVIntSize(RLE_MARKER)
            + WritableUtils.getVIntSize(valueLength);
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(0 + valueLength);
        }
      } else {
        WritableUtils.writeVInt(out, keyLength);
        WritableUtils.writeVInt(out, valueLength);
        out.write(key.getData(), key.getPosition(), keyLength);
        out.write(value.getData(), value.getPosition(), valueLength);

        // Update bytes written
        decompressedBytesWritten += keyLength + valueLength
            + WritableUtils.getVIntSize(keyLength)
            + WritableUtils.getVIntSize(valueLength);
        if (serializedUncompressedBytes != null) {
          serializedUncompressedBytes.increment(keyLength + valueLength);
        }
                
        BufferUtils.copy(key, previous);        
      }
      ++numRecordsWritten;
    }
    
    // Required for mark/reset
    public DataOutputStream getOutputStream () {
      return out;
    }
    
    // Required for mark/reset
    public void updateCountersForExternalAppend(long length) {
      ++numRecordsWritten;
      decompressedBytesWritten += length;
    }
    
    public long getRawLength() {
      return decompressedBytesWritten;
    }
    
    public long getCompressedLength() {
      return compressedBytesWritten;
    }
    
    public void setRLE(boolean rle) {
      this.rle = rle;
      previous.reset();
    }

  }

  /**
   * <code>IFile.Reader</code> to read intermediate map-outputs. 
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class Reader {
    
    public enum KeyState {NO_KEY, NEW_KEY, SAME_KEY};
    
    private static final int DEFAULT_BUFFER_SIZE = 128*1024;

    // Count records read from disk
    private long numRecordsRead = 0;
    private final TezCounter readRecordsCounter;
    private final TezCounter bytesReadCounter;

    final InputStream in;        // Possibly decompressed stream that we read
    Decompressor decompressor;
    public long bytesRead = 0;
    protected final long fileLength;
    protected boolean eof = false;
    final IFileInputStream checksumIn;
    
    protected byte[] buffer = null;
    protected int bufferSize = DEFAULT_BUFFER_SIZE;
    protected DataInputStream dataIn;

    protected int recNo = 1;
    protected int prevKeyLength;
    protected int currentKeyLength;
    protected int currentValueLength;
    byte keyBytes[] = new byte[0];
    
    long startPos;
    
    
    /**
     * Construct an IFile Reader.
     * 
     * @param fs  FileSystem
     * @param file Path of the file to be opened. This file should have
     *             checksum bytes for the data at the end of the file.
     * @param codec codec
     * @param readsCounter Counter for records read from disk
     * @throws IOException
     */
    public Reader(FileSystem fs, Path file,
                  CompressionCodec codec,
                  TezCounter readsCounter, TezCounter bytesReadCounter, boolean ifileReadAhead,
                  int ifileReadAheadLength, int bufferSize) throws IOException {
      this(fs.open(file), 
           fs.getFileStatus(file).getLen(),
           codec, readsCounter, bytesReadCounter, ifileReadAhead, ifileReadAheadLength, bufferSize);
    }

    /**
     * Construct an IFile Reader.
     * 
     * @param in   The input stream
     * @param length Length of the data in the stream, including the checksum
     *               bytes.
     * @param codec codec
     * @param readsCounter Counter for records read from disk
     * @throws IOException
     */
    public Reader(InputStream in, long length, 
                  CompressionCodec codec,
                  TezCounter readsCounter, TezCounter bytesReadCounter,
                  boolean readAhead, int readAheadLength,
                  int bufferSize) throws IOException {
      readRecordsCounter = readsCounter;
      this.bytesReadCounter = bytesReadCounter;
      checksumIn = new IFileInputStream(in,length, readAhead, readAheadLength);
      if (codec != null) {
        decompressor = CodecPool.getDecompressor(codec);
        if (decompressor != null) {
          this.in = codec.createInputStream(checksumIn, decompressor);
        } else {
          LOG.warn("Could not obtain decompressor from CodecPool");
          this.in = checksumIn;
        }
      } else {
        this.in = checksumIn;
      }
      this.dataIn = new DataInputStream(this.in);
      this.fileLength = length;
      
      startPos = checksumIn.getPosition();
      
      if (bufferSize != -1) {
        this.bufferSize = bufferSize;
      }
    }
    
    public long getLength() { 
      return fileLength - checksumIn.getSize();
    }
    
    public long getPosition() throws IOException {    
      return checksumIn.getPosition(); 
    }
    
    /**
     * Read upto len bytes into buf starting at offset off.
     * 
     * @param buf buffer 
     * @param off offset
     * @param len length of buffer
     * @return the no. of bytes read
     * @throws IOException
     */
    private int readData(byte[] buf, int off, int len) throws IOException {
      int bytesRead = 0;
      while (bytesRead < len) {
        int n = IOUtils.wrappedReadForCompressedData(in, buf, off + bytesRead,
            len - bytesRead);
        if (n < 0) {
          return bytesRead;
        }
        bytesRead += n;
      }
      return len;
    }
    
    protected boolean positionToNextRecord(DataInput dIn) throws IOException {
      // Sanity check
      if (eof) {
        throw new EOFException("Completed reading " + bytesRead);
      }
      
      // Read key and value lengths
      prevKeyLength = currentKeyLength;
      currentKeyLength = WritableUtils.readVInt(dIn);
      currentValueLength = WritableUtils.readVInt(dIn);
      bytesRead += WritableUtils.getVIntSize(currentKeyLength) +
                   WritableUtils.getVIntSize(currentValueLength);
      
      // Check for EOF
      if (currentKeyLength == EOF_MARKER && currentValueLength == EOF_MARKER) {
        eof = true;
        return false;
      }      
      
      // Sanity check
      if (currentKeyLength != RLE_MARKER && currentKeyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: " + 
                              currentKeyLength);
      }
      if (currentValueLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative value-length: " + 
                              currentValueLength);
      }
            
      return true;
    }
    
    public boolean nextRawKey(DataInputBuffer key) throws IOException {
      return readRawKey(key) != KeyState.NO_KEY;
    }
    
    public KeyState readRawKey(DataInputBuffer key) throws IOException {
      if (!positionToNextRecord(dataIn)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("currentKeyLength=" + currentKeyLength +
              ", currentValueLength=" + currentValueLength +
              ", bytesRead=" + bytesRead + 
              ", length=" + fileLength);
        }
        return KeyState.NO_KEY;
      }
      if(currentKeyLength == RLE_MARKER) {
        currentKeyLength = prevKeyLength;
        // no data to read
        key.reset(keyBytes, currentKeyLength);
        return KeyState.SAME_KEY;
      }
      if (keyBytes.length < currentKeyLength) {
        keyBytes = new byte[currentKeyLength << 1];
      }
      int i = readData(keyBytes, 0, currentKeyLength);
      if (i != currentKeyLength) {
        throw new IOException ("Asked for " + currentKeyLength + " Got: " + i);
      }
      key.reset(keyBytes, currentKeyLength);
      bytesRead += currentKeyLength;
      return KeyState.NEW_KEY;
    }
    
    public void nextRawValue(DataInputBuffer value) throws IOException {
      final byte[] valBytes = 
        ((value.getData().length < currentValueLength) || (value.getData() == keyBytes))
        ? new byte[currentValueLength << 1]
        : value.getData();
      int i = readData(valBytes, 0, currentValueLength);
      if (i != currentValueLength) {
        throw new IOException ("Asked for " + currentValueLength + " Got: " + i);
      }
      value.reset(valBytes, currentValueLength);
      
      // Record the bytes read
      bytesRead += currentValueLength;

      ++recNo;
      ++numRecordsRead;
    }
    
    public void close() throws IOException {
      // Close the underlying stream
      in.close();

      // Release the buffer
      dataIn = null;
      buffer = null;
      if(readRecordsCounter != null) {
        readRecordsCounter.increment(numRecordsRead);
      }

      if (bytesReadCounter != null) {
        bytesReadCounter.increment(checksumIn.getPosition() - startPos + checksumIn.getSize());
      }
      
      // Return the decompressor
      if (decompressor != null) {
        decompressor.reset();
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
    
    public void reset(int offset) {
      return;
    }

    public void disableChecksumValidation() {
      checksumIn.disableChecksumValidation();
    }

  }    
}
