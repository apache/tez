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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.tez.runtime.library.utils.BufferUtils;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.InMemoryReader;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.InMemoryWriter;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.testutils.KVDataGen;
import org.apache.tez.runtime.library.testutils.KVDataGen.KVPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestIFile {

  private static final Log LOG = LogFactory.getLog(TestIFile.class);

  private static Configuration defaultConf = new Configuration();
  private static FileSystem localFs = null;
  private static Path workDir = null;
  private static CompressionCodec codec;
  private Random rnd = new Random();
  private String outputFileName = "ifile.out";
  private Path outputPath;
  private DataOutputBuffer k = new DataOutputBuffer();
  private DataOutputBuffer v = new DataOutputBuffer();

  static {
    defaultConf.set("fs.defaultFS", "file:///");
    try {
      localFs = FileSystem.getLocal(defaultConf);
      workDir = new Path(
          new Path(System.getProperty("test.build.data", "/tmp")), TestIFile.class.getName())
          .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
      LOG.info("Using workDir: " + workDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setUp() throws Exception {
    CompressionCodecFactory codecFactory = new CompressionCodecFactory(new
        Configuration());
    codec = codecFactory.getCodecByClassName("org.apache.hadoop.io.compress.DefaultCodec");
    outputPath = new Path(workDir, outputFileName);
  }

  @Before
  @After
  public void cleanup() throws Exception {
    localFs.delete(workDir, true);
  }

  @Test(timeout = 5000)
  //empty IFile
  public void testWithEmptyIFile() throws IOException {
    testWriterAndReader(new LinkedList<KVPair>());
    testWithDataBuffer(new LinkedList<KVPair>());
  }

  @Test(timeout = 5000)
  public void testCompressedFlag() throws IOException {
    byte[] HEADER = new byte[] { (byte) 'T', (byte) 'I', (byte) 'F' , (byte) 1};
    ByteArrayInputStream bin = new ByteArrayInputStream(HEADER);
    boolean compressed = IFile.Reader.isCompressedFlagEnabled(bin);
    assert(compressed == true);

    //Negative case: Half cooked header
    HEADER = new byte[] { (byte) 'T', (byte) 'I' };
    bin = new ByteArrayInputStream(HEADER);
    try {
      compressed = IFile.Reader.isCompressedFlagEnabled(bin);
      fail("Should not have allowed wrong header");
    } catch(Exception e) {
      //correct path.
    }
  }

  @Test(timeout = 5000)
  //Write empty key value pairs
  public void testWritingEmptyKeyValues() throws IOException {
    DataInputBuffer key = new DataInputBuffer();
    DataInputBuffer value = new DataInputBuffer();
    IFile.Writer writer = new IFile.Writer(defaultConf, localFs, outputPath, null, null, null,
        null, null);
    writer.append(key, value);
    writer.append(key, value);
    writer.append(key, value);
    writer.append(key, value);
    writer.close();

    IFile.Reader reader = new Reader(localFs, outputPath, null, null, null, false, -1, 1024);
    DataInputBuffer keyIn = new DataInputBuffer();
    DataInputBuffer valIn = new DataInputBuffer();
    int records = 0;
    while (reader.nextRawKey(keyIn)) {
      reader.nextRawValue(valIn);
      records++;
      assert(keyIn.getLength() == 0);
      assert(valIn.getLength() == 0);
    }
    assertTrue("Number of records read does not match", (records == 4));
    reader.close();
  }

  @Test(timeout = 5000)
  //test with unsorted data and repeat keys
  public void testWithUnsortedData() throws IOException {
    List<KVPair> unsortedData = KVDataGen.generateTestData(false, rnd.nextInt(100));
    testWriterAndReader(unsortedData);
    testWithDataBuffer(unsortedData);
  }

  @Test(timeout = 5000)
  //test with sorted data and repeat keys
  public void testWithSortedData() throws IOException {
    List<KVPair> sortedData = KVDataGen.generateTestData(true, rnd.nextInt(100));
    testWriterAndReader(sortedData);
    testWithDataBuffer(sortedData);
  }


  @Test(timeout = 5000)
  //test with sorted data and repeat keys
  public void testWithRLEMarker() throws IOException {
    //Test with append(Object, Object)
    FSDataOutputStream out = localFs.create(outputPath);
    IFile.Writer writer = new IFile.Writer(defaultConf, out,
        Text.class, IntWritable.class, codec, null, null, true);

    Text key = new Text("key0");
    IntWritable value = new IntWritable(0);
    writer.append(key, value);

    //same key (RLE should kick in)
    key = new Text("key0");
    writer.append(key, value);
    assertTrue(writer.sameKey);

    //Different key
    key = new Text("key1");
    writer.append(key, value);
    assertFalse(writer.sameKey);
    writer.close();
    out.close();


    //Test with append(DataInputBuffer key, DataInputBuffer value)
    byte[] kvbuffer = "key1Value1key1Value2key3Value3".getBytes();
    int keyLength = 4;
    int valueLength = 6;
    int pos = 0;
    out = localFs.create(outputPath);
    writer = new IFile.Writer(defaultConf, out,
        Text.class, IntWritable.class, codec, null, null, true);

    DataInputBuffer kin = new DataInputBuffer();
    kin.reset(kvbuffer, pos, keyLength);

    DataInputBuffer vin = new DataInputBuffer();
    DataOutputBuffer vout = new DataOutputBuffer();
    (new IntWritable(0)).write(vout);
    vin.reset(vout.getData(), vout.getLength());

    //Write initial KV pair
    writer.append(kin, vin);
    assertFalse(writer.sameKey);
    pos += (keyLength + valueLength);

    //Second key is similar to key1 (RLE should kick in)
    kin.reset(kvbuffer, pos, keyLength);
    (new IntWritable(0)).write(vout);
    vin.reset(vout.getData(), vout.getLength());
    writer.append(kin, vin);
    assertTrue(writer.sameKey);
    pos += (keyLength + valueLength);

    //Next key (key3) is different (RLE should not kick in)
    kin.reset(kvbuffer, pos, keyLength);
    (new IntWritable(0)).write(vout);
    vin.reset(vout.getData(), vout.getLength());
    writer.append(kin, vin);
    assertFalse(writer.sameKey);

    writer.close();
    out.close();
  }

  @Test(timeout = 5000)
  //test with unique keys
  public void testWithUniqueKeys() throws IOException {
    //all keys are unique
    List<KVPair> sortedData = KVDataGen.generateTestData(true, 0);
    testWriterAndReader(sortedData);
    testWithDataBuffer(sortedData);
  }

  @Test(timeout = 5000)
  //Test InMemoryWriter
  public void testInMemoryWriter() throws IOException {
    InMemoryWriter writer = null;
    BoundedByteArrayOutputStream bout = new BoundedByteArrayOutputStream(1024 * 1024);

    List<KVPair> data = KVDataGen.generateTestData(true, 0);

    //No RLE, No RepeatKeys, no compression
    writer = new InMemoryWriter(bout);
    writeTestFileUsingDataBuffer(writer, false, false, data, null);
    readUsingInMemoryReader(bout.getBuffer(), data);

    //No RLE, RepeatKeys, no compression
    bout.reset();
    writer = new InMemoryWriter(bout);
    writeTestFileUsingDataBuffer(writer, false, true, data, null);
    readUsingInMemoryReader(bout.getBuffer(), data);

    //RLE, No RepeatKeys, no compression
    bout.reset();
    writer = new InMemoryWriter(bout);
    writeTestFileUsingDataBuffer(writer, true, false, data, null);
    readUsingInMemoryReader(bout.getBuffer(), data);

    //RLE, RepeatKeys, no compression
    bout.reset();
    writer = new InMemoryWriter(bout);
    writeTestFileUsingDataBuffer(writer, true, true, data, null);
    readUsingInMemoryReader(bout.getBuffer(), data);
  }

  @Test(timeout = 5000)
  //Test appendValue feature
  public void testAppendValue() throws IOException {
    List<KVPair> data = KVDataGen.generateTestData(false, rnd.nextInt(100));
    IFile.Writer writer = new IFile.Writer(defaultConf, localFs, outputPath,
        Text.class, IntWritable.class, codec, null, null);

    Text previousKey = null;
    for (KVPair kvp : data) {
      if ((previousKey != null && previousKey.compareTo(kvp.getKey()) == 0)) {
        writer.appendValue(kvp.getvalue());
      } else {
        writer.append(kvp.getKey(), kvp.getvalue());
      }
      previousKey = kvp.getKey();
    }

    writer.close();

    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);
  }

  @Test(timeout = 5000)
  //Test appendValues feature
  public void testAppendValues() throws IOException {
    List<KVPair> data = new ArrayList<KVPair>();
    List<IntWritable> values = new ArrayList<IntWritable>();

    Text key = new Text("key");
    IntWritable val = new IntWritable(1);
    for(int i = 0; i < 5; i++) {
      data.add(new KVPair(key, val));
      values.add(val);
    }

    IFile.Writer writer = new IFile.Writer(defaultConf, localFs, outputPath,
        Text.class, IntWritable.class, codec, null, null);
    writer.append(data.get(0).getKey(), data.get(0).getvalue()); //write first KV pair
    writer.appendValues(values.subList(1, values.size()).iterator()); //add the rest here

    Text lastKey = new Text("key3");
    IntWritable lastVal = new IntWritable(10);
    data.add(new KVPair(lastKey, lastVal));

    writer.append(lastKey, lastVal);
    writer.close();

    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);
  }

  @Test(timeout = 5000)
  //Test appendKeyValues feature
  public void testAppendKeyValues() throws IOException {
    List<KVPair> data = new ArrayList<KVPair>();
    List<IntWritable> values = new ArrayList<IntWritable>();

    Text key = new Text("key");
    IntWritable val = new IntWritable(1);
    for(int i = 0; i < 5; i++) {
      data.add(new KVPair(key, val));
      values.add(val);
    }

    IFile.Writer writer = new IFile.Writer(defaultConf, localFs, outputPath,
        Text.class, IntWritable.class, codec, null, null);
    writer.appendKeyValues(data.get(0).getKey(), values.iterator());

    Text lastKey = new Text("key3");
    IntWritable lastVal = new IntWritable(10);
    data.add(new KVPair(lastKey, lastVal));

    writer.append(lastKey, lastVal);
    writer.close();

    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);
  }

  @Test(timeout = 5000)
  //Test appendValue with DataInputBuffer
  public void testAppendValueWithDataInputBuffer() throws IOException {
    List<KVPair> data = KVDataGen.generateTestData(false, rnd.nextInt(100));
    IFile.Writer writer = new IFile.Writer(defaultConf, localFs, outputPath,
        Text.class, IntWritable.class, codec, null, null);

    final DataInputBuffer previousKey = new DataInputBuffer();
    DataInputBuffer key = new DataInputBuffer();
    DataInputBuffer value = new DataInputBuffer();
    for (KVPair kvp : data) {
      populateData(kvp, key, value);

      if ((previousKey != null && BufferUtils.compare(key, previousKey) == 0)) {
        writer.appendValue(value);
      } else {
        writer.append(key, value);
      }
      previousKey.reset(k.getData(), 0, k.getLength());
    }

    writer.close();

    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);
  }


  /**
   * Test different options (RLE, repeat keys, compression) on reader/writer
   *
   * @param data
   * @throws IOException
   */
  private void testWriterAndReader(List<KVPair> data) throws IOException {
    Writer writer = null;
    //No RLE, No RepeatKeys
    writer = writeTestFile(false, false, data, null);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, null);

    writer = writeTestFile(false, false, data, codec);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);

    //No RLE, RepeatKeys
    writer = writeTestFile(false, true, data, null);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, null);

    writer = writeTestFile(false, true, data, codec);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);

    //RLE, No RepeatKeys
    writer = writeTestFile(true, false, data, null);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, null);

    writer = writeTestFile(true, false, data, codec);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);

    //RLE, RepeatKeys
    writer = writeTestFile(true, true, data, null);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, null);

    writer = writeTestFile(true, true, data, codec);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);
  }

  /**
   * Test different options (RLE, repeat keys, compression) on reader/writer
   *
   * @param data
   * @throws IOException
   */
  private void testWithDataBuffer(List<KVPair> data) throws
      IOException {
    Writer writer = null;
    //No RLE, No RepeatKeys
    writer = writeTestFileUsingDataBuffer(false, false, data, null);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, null);

    writer = writeTestFileUsingDataBuffer(false, false, data, codec);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);

    //No RLE, RepeatKeys
    writer = writeTestFileUsingDataBuffer(false, true, data, null);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, null);

    writer = writeTestFileUsingDataBuffer(false, true, data, codec);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);

    //RLE, No RepeatKeys
    writer = writeTestFileUsingDataBuffer(true, false, data, null);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, null);

    writer = writeTestFileUsingDataBuffer(true, false, data, codec);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);

    //RLE, RepeatKeys
    writer = writeTestFileUsingDataBuffer(true, true, data, null);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, null);

    writer = writeTestFileUsingDataBuffer(true, true, data, codec);
    readAndVerifyData(writer.getRawLength(), writer.getCompressedLength(), data, codec);
  }

  private void readAndVerifyData(long rawLength, long compressedLength,
      List<KVPair> originalData, CompressionCodec codec) throws
      IOException {
    readFileUsingInMemoryReader(rawLength, compressedLength, originalData);
    readUsingIFileReader(originalData, codec);
  }

  /**
   * Read data using in memory reader
   *
   * @param rawLength
   * @param compressedLength
   * @param originalData
   * @throws IOException
   */
  private void readFileUsingInMemoryReader(long rawLength, long compressedLength,
      List<KVPair> originalData) throws IOException {
    LOG.info("Read using in memory reader");
    FSDataInputStream inStream = localFs.open(outputPath);
    byte[] bytes = new byte[(int) rawLength];

    IFile.Reader.readToMemory(bytes, inStream,
        (int) compressedLength, codec, false, -1);
    inStream.close();

    readUsingInMemoryReader(bytes, originalData);
  }

  private void readUsingInMemoryReader(byte[] bytes, List<KVPair> originalData)
      throws IOException {
    InMemoryReader inMemReader = new InMemoryReader(null,
        new InputAttemptIdentifier(0, 0), bytes, 0, bytes.length);
    verifyData(inMemReader, originalData);
  }

  /**
   * Read data using IFile Reader
   *
   * @param originalData
   * @param codec
   * @throws IOException
   */
  private void readUsingIFileReader(List<KVPair> originalData,
      CompressionCodec codec) throws IOException {
    LOG.info("Read using IFile reader");
    IFile.Reader reader = new IFile.Reader(localFs, outputPath,
        codec, null, null, false, 0, -1);
    verifyData(reader, originalData);
    reader.close();
  }

  /**
   * Data verification
   *
   * @param reader
   * @param data
   * @throws IOException
   */
  private void verifyData(Reader reader, List<KVPair> data)
      throws IOException {
    LOG.info("Data verification");
    Text readKey = new Text();
    IntWritable readValue = new IntWritable();
    DataInputBuffer keyIn = new DataInputBuffer();
    DataInputBuffer valIn = new DataInputBuffer();
    Deserializer<Text> keyDeserializer;
    Deserializer<IntWritable> valDeserializer;
    SerializationFactory serializationFactory = new SerializationFactory(
        defaultConf);
    keyDeserializer = serializationFactory.getDeserializer(Text.class);
    valDeserializer = serializationFactory.getDeserializer(IntWritable.class);
    keyDeserializer.open(keyIn);
    valDeserializer.open(valIn);

    int numRecordsRead = 0;

    while (reader.nextRawKey(keyIn)) {
      reader.nextRawValue(valIn);
      readKey = keyDeserializer.deserialize(readKey);
      readValue = valDeserializer.deserialize(readValue);

      KVPair expected = data.get(numRecordsRead);
      assertEquals("Key does not match: Expected: " + expected.getKey()
          + ", Read: " + readKey, expected.getKey(), readKey);
      assertEquals("Value does not match: Expected: " + expected.getvalue()
          + ", Read: " + readValue, expected.getvalue(), readValue);

      numRecordsRead++;
    }
    assertEquals("Expected: " + data.size() + " records, but found: "
        + numRecordsRead, data.size(), numRecordsRead);
    LOG.info("Found: " + numRecordsRead + " records");
  }

  private Writer writeTestFile(boolean rle, boolean repeatKeys,
      List<KVPair> data, CompressionCodec codec) throws IOException {
    FSDataOutputStream out = localFs.create(outputPath);
    IFile.Writer writer = new IFile.Writer(defaultConf, out,
        Text.class, IntWritable.class, codec, null, null, rle);
    writeTestFile(writer, rle, repeatKeys, data, codec);
    out.close();
    return  writer;
  }

  private Writer writeTestFile(IFile.Writer writer, boolean rle, boolean repeatKeys,
      List<KVPair> data, CompressionCodec codec) throws IOException {
    assertNotNull(writer);

    Text previousKey = null;
    for (KVPair kvp : data) {
      if (repeatKeys && (previousKey != null && previousKey.compareTo(kvp.getKey()) == 0)) {
        //RLE is enabled in IFile when IFile.REPEAT_KEY is set
        writer.append(IFile.REPEAT_KEY, kvp.getvalue());
      } else {
        writer.append(kvp.getKey(), kvp.getvalue());
      }
      previousKey = kvp.getKey();
    }

    writer.close();

    LOG.info("Uncompressed: " + writer.getRawLength());
    LOG.info("CompressedSize: " + writer.getCompressedLength());

    return writer;
  }

  private Writer writeTestFileUsingDataBuffer(boolean rle, boolean repeatKeys,
      List<KVPair> data, CompressionCodec codec) throws IOException {
    FSDataOutputStream out = localFs.create(outputPath);
    IFile.Writer writer = new IFile.Writer(defaultConf, out,
        Text.class, IntWritable.class, codec, null, null, rle);
    writeTestFileUsingDataBuffer(writer, rle, repeatKeys, data, codec);
    out.close();
    return writer;
  }

  private Writer writeTestFileUsingDataBuffer(IFile.Writer writer, boolean rle, boolean repeatKeys,
      List<KVPair> data, CompressionCodec codec) throws IOException {
    DataInputBuffer previousKey = new DataInputBuffer();
    DataInputBuffer key = new DataInputBuffer();
    DataInputBuffer value = new DataInputBuffer();
    for (KVPair kvp : data) {
      populateData(kvp, key, value);

      if (repeatKeys && (previousKey != null && BufferUtils.compare(key, previousKey) == 0)) {
        writer.append(IFile.REPEAT_KEY, value);
      } else {
        writer.append(key, value);
      }
      previousKey.reset(k.getData(), 0, k.getLength());
    }

    writer.close();

    LOG.info("Uncompressed: " + writer.getRawLength());
    LOG.info("CompressedSize: " + writer.getCompressedLength());

    return writer;
  }

  private void populateData(KVPair kvp, DataInputBuffer key, DataInputBuffer value)
      throws  IOException {
    DataOutputBuffer k = new DataOutputBuffer();
    DataOutputBuffer v = new DataOutputBuffer();
    kvp.getKey().write(k);
    kvp.getvalue().write(v);
    key.reset(k.getData(), 0, k.getLength());
    value.reset(v.getData(), 0, v.getLength());
  }
}
