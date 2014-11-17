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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.tez.runtime.library.utils.BufferUtils;
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
  public static final int V_END_MARKER = -3; // End of values marker
  public static final DataInputBuffer REPEAT_KEY = new DataInputBuffer();
  public static final byte[] HEADER = new byte[] { (byte) 'T', (byte) 'I',
    (byte) 'F' , (byte) 0};

  private static final String WRONG_KEY_CLASS = "wrong key class: %s is not %s";
  private static final String WRONG_VALUE_CLASS = "wrong value class: %s is not %s";
  private static final String NEGATIVE_KEY_LEN = "Negative key-length not allowed: %d for %s";
  private static final String NEGATIVE_VAL_LEN = "Negative value-length not allowed: %d for %s";
  private static final String INCOMPLETE_READ = "Requested to read %d got %d";

  /**
   * <code>IFile.Writer</code> to write out intermediate map-outputs.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static class Writer {
    protected DataOutputStream out;
    boolean ownOutputStream = false;
    long start = 0;
    FSDataOutputStream rawOut;
    final AtomicBoolean closed = new AtomicBoolean(false);

    CompressionOutputStream compressedOut;
    Compressor compressor;
    boolean compressOutput = false;

    long decompressedBytesWritten = 0;
    long compressedBytesWritten = 0;

    // Count records written to disk
    private long numRecordsWritten = 0;
    private long rleWritten = 0; //number of RLE markers written
    private long totalKeySaving = 0; //number of keys saved due to multi KV writes + RLE
    private final TezCounter writtenRecordsCounter;
    private final TezCounter serializedUncompressedBytes;

    IFileOutputStream checksumOut;

    Class keyClass;
    Class valueClass;
    Serializer keySerializer;
    Serializer valueSerializer;

    final DataOutputBuffer buffer = new DataOutputBuffer();
    final DataOutputBuffer previous = new DataOutputBuffer();
    Object prevKey = null;
    boolean headerWritten = false;
    @VisibleForTesting
    boolean sameKey = false;

    final int RLE_MARKER_SIZE = WritableUtils.getVIntSize(RLE_MARKER);
    final int V_END_MARKER_SIZE = WritableUtils.getVIntSize(V_END_MARKER);

    // de-dup keys or not
    protected final boolean rle;


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
      this.rle = false;
    }

    public Writer(Configuration conf, FSDataOutputStream outputStream,
        Class keyClass, Class valueClass, CompressionCodec codec, TezCounter writesCounter,
        TezCounter serializedBytesCounter) throws IOException {
      this(conf, outputStream, keyClass, valueClass, codec, writesCounter,
          serializedBytesCounter, false);
    }

    public Writer(Configuration conf, FSDataOutputStream outputStream,
        Class keyClass, Class valueClass,
        CompressionCodec codec, TezCounter writesCounter, TezCounter serializedBytesCounter,
        boolean rle) throws IOException {
      this.rawOut = outputStream;
      this.writtenRecordsCounter = writesCounter;
      this.serializedUncompressedBytes = serializedBytesCounter;
      this.checksumOut = new IFileOutputStream(outputStream);
      this.start = this.rawOut.getPos();
      this.rle = rle;
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
      writeHeader(outputStream);
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

    public Writer(Configuration conf, FileSystem fs, Path file) throws IOException {
      this(conf, fs, file, null, null, null, null, null);
    }

    protected void writeHeader(OutputStream outputStream) throws IOException {
      if (!headerWritten) {
        outputStream.write(HEADER, 0, HEADER.length - 1);
        outputStream.write((compressOutput) ? (byte) 1 : (byte) 0);
        outputStream.flush();
        headerWritten = true;
      }
    }

    public void close() throws IOException {
      checkState(!closed.getAndSet(true), "Writer was already closed earlier");

      // When IFile writer is created by BackupStore, we do not have
      // Key and Value classes set. So, check before closing the
      // serializers
      if (keyClass != null) {
        keySerializer.close();
        valueSerializer.close();
      }

      // write V_END_MARKER as needed
      writeValueMarker(out);

      // Write EOF_MARKER for key/value length
      WritableUtils.writeVInt(out, EOF_MARKER);
      WritableUtils.writeVInt(out, EOF_MARKER);
      decompressedBytesWritten += 2 * WritableUtils.getVIntSize(EOF_MARKER);
      //account for header bytes
      decompressedBytesWritten += HEADER.length;

      //Flush the stream
      out.flush();

      // Close the underlying stream iff we own it...
      if (ownOutputStream) {
        out.close();
      } else {
        if (compressOutput) {
          // Flush
          compressedOut.finish();
          compressedOut.resetState();
        }
        // Write the checksum
        checksumOut.finish();
      }
      //header bytes are already included in rawOut
      compressedBytesWritten = rawOut.getPos() - start;

      if (compressOutput) {
        // Return back the compressor
        CodecPool.returnCompressor(compressor);
        compressor = null;
      }

      out = null;
      if (writtenRecordsCounter != null) {
        writtenRecordsCounter.increment(numRecordsWritten);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Total keys written=" + numRecordsWritten + "; rleEnabled=" + rle + "; Savings" +
            "(due to multi-kv/rle)=" + totalKeySaving + "; number of RLEs written=" +
            rleWritten + "; compressedLen=" + compressedBytesWritten + "; rawLen="
            + decompressedBytesWritten);
      }
    }

    /**
     * Send key/value to be appended to IFile. To represent same key as previous
     * one, send IFile.REPEAT_KEY as key parameter.  Should not call this method with
     * IFile.REPEAT_KEY as the first key.
     *
     * @param key
     * @param value
     * @throws IOException
     */
    public void append(Object key, Object value) throws IOException {
      checkArgument((key == REPEAT_KEY || key.getClass() == keyClass), WRONG_KEY_CLASS,
          key.getClass(), keyClass);
      checkArgument((value.getClass() == valueClass), WRONG_VALUE_CLASS, value.getClass(),
          valueClass);

      int keyLength = 0;
      sameKey = (key == REPEAT_KEY);
      if (!sameKey) {
        keySerializer.serialize(key);
        keyLength = buffer.getLength();
        checkState(keyLength >= 0, NEGATIVE_KEY_LEN, keyLength, key);
        if (rle && (keyLength == previous.getLength())) {
          sameKey = (BufferUtils.compare(previous, buffer) == 0);
        }
      }

      // Append the 'value'
      valueSerializer.serialize(value);
      int valueLength = buffer.getLength() - keyLength;
      checkState(valueLength >= 0, NEGATIVE_VAL_LEN, valueLength, value);
      if (!sameKey) {
        //dump entire key value pair
        writeKVPair(buffer.getData(), 0, keyLength, buffer.getData(),
            keyLength, buffer.getLength() - keyLength);
        if (rle) {
          previous.reset();
          previous.write(buffer.getData(), 0, keyLength); //store the key
        }
      } else {
        writeValue(buffer.getData(), keyLength, valueLength);
      }

      prevKey = (sameKey) ? REPEAT_KEY : key;
      // Reset
      buffer.reset();
      ++numRecordsWritten;
    }

    /**
     * Appends the value to previous key. Assumes that the caller has already done relevant checks
     * for identical keys. Also, no validations are done in this method
     *
     * @param value
     * @throws IOException
     */
    public void appendValue(Object value) throws IOException {
      valueSerializer.serialize(value);
      int valueLength = buffer.getLength();
      checkState(valueLength >= 0, NEGATIVE_VAL_LEN, valueLength, value);
      writeValue(buffer.getData(), 0, valueLength);
      buffer.reset();
      ++numRecordsWritten;
      prevKey = REPEAT_KEY;
    }

    /**
     * Appends the value to previous key. Assumes that the caller has already done relevant checks
     * for identical keys. Also, no validations are done in this method
     *
     * @param value
     * @throws IOException
     */
    public void appendValue(DataInputBuffer value) throws IOException {
      int valueLength = value.getLength() - value.getPosition();
      checkState(valueLength >= 0, NEGATIVE_VAL_LEN, valueLength, value);
      writeValue(value.getData(), value.getPosition(), valueLength);
      buffer.reset();
      ++numRecordsWritten;
      prevKey = REPEAT_KEY;
    }

    /**
     * Appends the value to previous key. Assumes that the caller has already done relevant checks
     * for identical keys. Also, no validations are done in this method
     *
     * @param valuesItr
     * @throws IOException
     */
    public <V> void appendValues(Iterator<V> valuesItr) throws IOException {
      while(valuesItr.hasNext()) {
        appendValue(valuesItr.next());
      }
    }

    /**
     * Append key and its associated set of values.
     *
     * @param key
     * @param valuesItr
     * @param <K>
     * @param <V>
     * @throws IOException
     */
    public <K, V> void appendKeyValues(K key, Iterator<V> valuesItr) throws IOException {
      if (valuesItr.hasNext()) {
        append(key, valuesItr.next()); //append first KV pair
      }
      //append the remaining values
      while(valuesItr.hasNext()) {
        appendValue(valuesItr.next());
      }
    }

    /**
     * Send key/value to be appended to IFile. To represent same key as previous
     * one, send IFile.REPEAT_KEY as key parameter.  Should not call this method with
     * IFile.REPEAT_KEY as the first key.
     *
     * @param key
     * @param value
     * @throws IOException
     */
    public void append(DataInputBuffer key, DataInputBuffer value) throws IOException {
      int keyLength = key.getLength() - key.getPosition();
      checkState((key == REPEAT_KEY || keyLength >= 0), NEGATIVE_KEY_LEN, keyLength, key);

      int valueLength = value.getLength() - value.getPosition();
      checkState(valueLength >= 0, NEGATIVE_VAL_LEN, valueLength, value);

      sameKey = (key == REPEAT_KEY);
      if (!sameKey && rle) {
        sameKey = (keyLength != 0) && (BufferUtils.compare(previous, key) == 0);
      }

      if (!sameKey) {
        writeKVPair(key.getData(), key.getPosition(), keyLength,
            value.getData(), value.getPosition(), valueLength);
        if (rle) {
          BufferUtils.copy(key, previous);
        }
      } else {
        writeValue(value.getData(), value.getPosition(), valueLength);
      }
      prevKey = (sameKey) ? REPEAT_KEY : key;
      ++numRecordsWritten;
    }

    protected void writeValue(byte[] data, int offset, int length) throws IOException {
      writeRLE(out);
      WritableUtils.writeVInt(out, length); // value length
      out.write(data, offset, length);
      // Update bytes written
      decompressedBytesWritten +=
          length + WritableUtils.getVIntSize(length);
      if (serializedUncompressedBytes != null) {
        serializedUncompressedBytes.increment(length);
      }
      totalKeySaving++;
    }

    protected void writeKVPair(byte[] keyData, int keyPos, int keyLength,
        byte[] valueData, int valPos, int valueLength) throws IOException {
      writeValueMarker(out);
      WritableUtils.writeVInt(out, keyLength);
      WritableUtils.writeVInt(out, valueLength);
      out.write(keyData, keyPos, keyLength);
      out.write(valueData, valPos, valueLength);

      // Update bytes written
      decompressedBytesWritten +=
          keyLength + valueLength + WritableUtils.getVIntSize(keyLength)
              + WritableUtils.getVIntSize(valueLength);
      if (serializedUncompressedBytes != null) {
        serializedUncompressedBytes.increment(keyLength + valueLength);
      }
    }

    protected void writeRLE(DataOutputStream out) throws IOException {
      /**
       * To strike a balance between 2 use cases (lots of unique KV in stream
       * vs lots of identical KV in stream), we start off by writing KV pair.
       * If subsequent KV is identical, we write RLE marker along with V_END_MARKER
       * {KL1, VL1, K1, V1}
       * {RLE, VL2, V2, VL3, V3, ...V_END_MARKER}
       */
      if (prevKey != REPEAT_KEY) {
        WritableUtils.writeVInt(out, RLE_MARKER);
        decompressedBytesWritten += RLE_MARKER_SIZE;
        rleWritten++;
      }
    }

    protected void writeValueMarker(DataOutputStream out) throws IOException {
      /**
       * Write V_END_MARKER only in RLE scenario. This will
       * save space in conditions where lots of unique KV pairs are found in the
       * stream.
       */
      if (prevKey == REPEAT_KEY) {
        WritableUtils.writeVInt(out, V_END_MARKER);
        decompressedBytesWritten += V_END_MARKER_SIZE;
      }
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
  }

  /**
   * <code>IFile.Reader</code> to read intermediate map-outputs.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static class Reader {

    public enum KeyState {NO_KEY, NEW_KEY, SAME_KEY}

    private static final int DEFAULT_BUFFER_SIZE = 128*1024;

    // Count records read from disk
    private long numRecordsRead = 0;
    private final TezCounter readRecordsCounter;
    private final TezCounter bytesReadCounter;

    final InputStream in;        // Possibly decompressed stream that we read
    Decompressor decompressor;
    public long bytesRead = 0;
    final long fileLength;
    protected boolean eof = false;
    IFileInputStream checksumIn;

    protected byte[] buffer = null;
    protected int bufferSize = DEFAULT_BUFFER_SIZE;
    protected DataInputStream dataIn;

    protected int recNo = 1;
    protected int originalKeyLength;
    protected int prevKeyLength;
    protected int currentKeyLength;
    protected int currentValueLength;
    byte keyBytes[] = new byte[0];

    long startPos;
    protected boolean isCompressed = false;

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
      this(fs.open(file), fs.getFileStatus(file).getLen(), codec,
          readsCounter, bytesReadCounter, ifileReadAhead,
          ifileReadAheadLength, bufferSize);
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
      this(in, ((in != null) ? (length - HEADER.length) : length), codec,
          readsCounter, bytesReadCounter, readAhead, readAheadLength,
          bufferSize, ((in != null) ? isCompressedFlagEnabled(in) : false));
      if (in != null && bytesReadCounter != null) {
        bytesReadCounter.increment(IFile.HEADER.length);
      }
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
                  int bufferSize, boolean isCompressed) throws IOException {
      this.isCompressed = isCompressed;
      checksumIn = new IFileInputStream(in, length, readAhead, readAheadLength/*, isCompressed*/);
      if (isCompressed && codec != null) {
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
      startPos = checksumIn.getPosition();
      this.readRecordsCounter = readsCounter;
      this.bytesReadCounter = bytesReadCounter;
      this.fileLength = length;
      this.bufferSize = Math.max(0, bufferSize);
    }

    /**
     * Read entire ifile content to memory.
     *
     * @param buffer
     * @param in
     * @param compressedLength
     * @param codec
     * @param ifileReadAhead
     * @param ifileReadAheadLength
     * @throws IOException
     */
    public static void readToMemory(byte[] buffer, InputStream in, int compressedLength,
        CompressionCodec codec, boolean ifileReadAhead, int ifileReadAheadLength)
        throws IOException {
      boolean isCompressed = IFile.Reader.isCompressedFlagEnabled(in);
      IFileInputStream checksumIn = new IFileInputStream(in,
          compressedLength - IFile.HEADER.length, ifileReadAhead,
          ifileReadAheadLength);
      in = checksumIn;
      Decompressor decompressor = null;
      if (isCompressed && codec != null) {
        decompressor = CodecPool.getDecompressor(codec);
        if (decompressor != null) {
          decompressor.reset();
          in = codec.createInputStream(checksumIn, decompressor);
        } else {
          LOG.warn("Could not obtain decompressor from CodecPool");
          in = checksumIn;
        }
      }
      try {
        IOUtils.readFully(in, buffer, 0, buffer.length - IFile.HEADER.length);
      } catch (IOException ioe) {
        IOUtils.cleanup(LOG, in);
        throw ioe;
      } finally {
        if (decompressor != null) {
          decompressor.reset();
          CodecPool.returnDecompressor(decompressor);
        }
      }
    }

    public long getLength() {
      return fileLength - checksumIn.getSize();
    }

    public long getPosition() throws IOException {
      return checksumIn.getPosition();
    }

    /**
     * Read up to len bytes into buf starting at offset off.
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

    protected void readValueLength(DataInput dIn) throws IOException {
      currentValueLength = WritableUtils.readVInt(dIn);
      bytesRead += WritableUtils.getVIntSize(currentValueLength);
      if (currentValueLength == V_END_MARKER) {
        readKeyValueLength(dIn);
      }
    }

    protected void readKeyValueLength(DataInput dIn) throws IOException {
      currentKeyLength = WritableUtils.readVInt(dIn);
      currentValueLength = WritableUtils.readVInt(dIn);
      if (currentKeyLength != RLE_MARKER) {
        // original key length
        originalKeyLength = currentKeyLength;
      }
      bytesRead +=
          WritableUtils.getVIntSize(currentKeyLength)
              + WritableUtils.getVIntSize(currentValueLength);
    }

    /**
     * Reset key length and value length for next record in the file
     *
     * @param dIn
     * @return true if key length and value length were set to the next
     *         false if end of file (EOF) marker was reached
     * @throws IOException
     */
    protected boolean positionToNextRecord(DataInput dIn) throws IOException {
      // Sanity check
      checkState(!eof, "Reached EOF. Completed reading %d", bytesRead);
      prevKeyLength = currentKeyLength;

      if (prevKeyLength == RLE_MARKER) {
        // Same key as previous one. Just read value length alone
        readValueLength(dIn);
      } else {
        readKeyValueLength(dIn);
      }

      // Check for EOF
      if (currentKeyLength == EOF_MARKER && currentValueLength == EOF_MARKER) {
        eof = true;
        return false;
      }

      // Sanity check
      if (currentKeyLength != RLE_MARKER && currentKeyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: " +
                              currentKeyLength + " PreviousKeyLen: " + prevKeyLength);
      }
      if (currentValueLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative value-length: " +
                              currentValueLength);
      }
      return true;
    }

    public final boolean nextRawKey(DataInputBuffer key) throws IOException {
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
        // get key length from original key
        key.reset(keyBytes, originalKeyLength);
        return KeyState.SAME_KEY;
      }
      if (keyBytes.length < currentKeyLength) {
        keyBytes = new byte[currentKeyLength << 1];
      }
      int i = readData(keyBytes, 0, currentKeyLength);
      checkState((i == currentKeyLength), INCOMPLETE_READ, currentKeyLength, i);
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
      checkState((i == currentValueLength), INCOMPLETE_READ, currentValueLength, i);
      value.reset(valBytes, currentValueLength);

      // Record the bytes read
      bytesRead += currentValueLength;

      ++recNo;
      ++numRecordsRead;
    }

    public static boolean isCompressedFlagEnabled(InputStream in) throws IOException {
      byte[] header = new byte[HEADER.length];
      IOUtils.readFully(in, header, 0, HEADER.length);

      if (!(header[0] == 'T' && header[1] == 'I'
          && header[2] == 'F')) {
        throw new IOException("Not a valid ifile header");
      }
      return (header[3] == 1);
    }

    public void close() throws IOException {
      // Close the underlying stream
      in.close();

      // Release the buffer
      dataIn = null;
      buffer = null;
      if (readRecordsCounter != null) {
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

  public static void checkState(boolean expression, String format,
      Object... args) throws IOException {
    if (!expression) {
      throw new IOException(String.format(format, args));
    }
  }

  static void checkArgument(boolean expression, String format,
      Object... args) throws IOException {
    if (!expression) {
      throw new IOException(String.format(format, args));
    }
  }
}
