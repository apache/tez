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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import com.google.protobuf.ByteString;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutputFiles;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
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

  private static final Logger LOG = LoggerFactory.getLogger(TestIFile.class);

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
      defaultConf.set(TezRuntimeFrameworkConfigs.LOCAL_DIRS, workDir.toString());
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
  //test overflow
  public void testExceedMaxSize() throws IOException {
    final int oldMaxBufferSize = IFile.Reader.MAX_BUFFER_SIZE;

    Text shortString = new Text("string");
    Text longString = new Text("A string of length 22.");
    assertEquals(22, longString.getLength());

    Text readKey = new Text();
    Text readValue = new Text();
    DataInputBuffer keyIn = new DataInputBuffer();
    DataInputBuffer valIn = new DataInputBuffer();

    IFile.Writer writer;
    IFile.Reader reader;
    FSDataOutputStream out;

    // Check Key length exceeding MAX_BUFFER_SIZE
    out = localFs.create(outputPath);
    writer = new IFile.Writer(defaultConf, out,
            Text.class, Text.class, null, null, null, false);
    writer.append(longString, shortString);
    writer.close();

    out.close();

    // Set this to a smaller value for testing
    IFile.Reader.MAX_BUFFER_SIZE = 16;

    reader = new IFile.Reader(localFs, outputPath,
            null, null, null, false, 0, -1);

    try {
      reader.nextRawKey(keyIn);
      Assert.fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      // test passed
    }
    reader.close();

    // Check Value length exceeding MAX_BUFFER_SIZE
    out = localFs.create(outputPath);
    writer = new IFile.Writer(defaultConf, out,
            Text.class, Text.class, null, null, null, false);
    writer.append(shortString, longString);
    writer.close();

    out.close();

    // Set this to a smaller value for testing
    IFile.Reader.MAX_BUFFER_SIZE = 16;

    reader = new IFile.Reader(localFs, outputPath,
            null, null, null, false, 0, -1);

    try {
      reader.nextRawKey(keyIn);
      reader.nextRawValue(valIn);
      Assert.fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException e) {
      // test passed
    }
    reader.close();

    // Check Key length not getting doubled
    out = localFs.create(outputPath);
    writer = new IFile.Writer(defaultConf, out,
            Text.class, Text.class, null, null, null, false);
    writer.append(longString, shortString);
    writer.close();

    out.close();

    // Set this to a smaller value for testing
    IFile.Reader.MAX_BUFFER_SIZE = 32;

    reader = new IFile.Reader(localFs, outputPath,
            null, null, null, false, 0, -1);

    reader.nextRawKey(keyIn);
    assertEquals(longString.getLength() + 1, keyIn.getData().length);
    reader.close();

    // Check Value length not getting doubled
    out = localFs.create(outputPath);
    writer = new IFile.Writer(defaultConf, out,
            Text.class, Text.class, null, null, null, false);
    writer.append(shortString, longString);
    writer.close();

    out.close();

    // Set this to a smaller value for testing
    IFile.Reader.MAX_BUFFER_SIZE = 32;

    reader = new IFile.Reader(localFs, outputPath,
            null, null, null, false, 0, -1);

    reader.nextRawKey(keyIn);
    reader.nextRawValue(valIn);
    assertEquals(longString.getLength() + 1, valIn.getData().length);
    reader.close();

    // revert back to original value
    IFile.Reader.MAX_BUFFER_SIZE = oldMaxBufferSize;
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

    BoundedByteArrayOutputStream boundedOut = new BoundedByteArrayOutputStream(1024*1024);
    Writer inMemWriter = new InMemoryWriter(boundedOut, true);

    DataInputBuffer kin = new DataInputBuffer();
    kin.reset(kvbuffer, pos, keyLength);

    DataInputBuffer vin = new DataInputBuffer();
    DataOutputBuffer vout = new DataOutputBuffer();
    (new IntWritable(0)).write(vout);
    vin.reset(vout.getData(), vout.getLength());

    //Write initial KV pair
    writer.append(kin, vin);
    assertFalse(writer.sameKey);
    inMemWriter.append(kin, vin);
    assertFalse(inMemWriter.sameKey);
    pos += (keyLength + valueLength);

    //Second key is similar to key1 (RLE should kick in)
    kin.reset(kvbuffer, pos, keyLength);
    (new IntWritable(0)).write(vout);
    vin.reset(vout.getData(), vout.getLength());
    writer.append(kin, vin);
    assertTrue(writer.sameKey);
    inMemWriter.append(kin, vin);
    assertTrue(inMemWriter.sameKey);
    pos += (keyLength + valueLength);

    //Next key (key3) is different (RLE should not kick in)
    kin.reset(kvbuffer, pos, keyLength);
    (new IntWritable(0)).write(vout);
    vin.reset(vout.getData(), vout.getLength());
    writer.append(kin, vin);
    assertFalse(writer.sameKey);
    inMemWriter.append(kin, vin);
    assertFalse(inMemWriter.sameKey);

    writer.close();
    out.close();
    inMemWriter.close();
    boundedOut.close();
  }

  @Test(timeout = 5000)
  //test with unique keys
  public void testWithUniqueKeys() throws IOException {
    //all keys are unique
    List<KVPair> sortedData = KVDataGen.generateTestData(true, 0);
    testWriterAndReader(sortedData);
    testWithDataBuffer(sortedData);
  }

  //test concatenated zlib input - as in multiple map outputs during shuffle
  //This specific input is valid but the decompressor can leave lingering
  // bytes between segments. If the lingering bytes aren't handled correctly,
  // the stream will get out-of-sync.
  @Test(timeout = 5000)
  public void testConcatenatedZlibPadding()
      throws IOException, URISyntaxException {
    byte[] bytes;
    long compTotal = 0;
    // Known raw and compressed lengths of input
    long raws[] = { 2392, 102314, 42576, 31432, 25090 };
    long compressed[] = { 723, 25396, 10926, 8203, 6665 };

    CompressionCodecFactory codecFactory = new CompressionCodecFactory(new
        Configuration());
    codec = codecFactory.getCodecByClassName("org.apache.hadoop.io.compress.DefaultCodec");

    URL url = getClass().getClassLoader()
        .getResource("TestIFile_concatenated_compressed.bin");
    assertNotEquals("IFileinput file must exist", null, url);
    Path p = new Path(url.toURI());
    FSDataInputStream inStream = localFs.open(p);

    for (int i = 0; i < 5; i++) {
      bytes = new byte[(int) raws[i]];
      assertEquals("Compressed stream out-of-sync", inStream.getPos(), compTotal);
      IFile.Reader.readToMemory(bytes, inStream, (int) compressed[i], codec,
          false, -1);
      compTotal += compressed[i];

      // Now read the data
      InMemoryReader inMemReader = new InMemoryReader(null,
          new InputAttemptIdentifier(0, 0), bytes, 0, bytes.length);

      DataInputBuffer keyIn = new DataInputBuffer();
      DataInputBuffer valIn = new DataInputBuffer();
      Deserializer<Text> keyDeserializer;
      Deserializer<IntWritable> valDeserializer;
      SerializationFactory serializationFactory =
          new SerializationFactory(defaultConf);
      keyDeserializer = serializationFactory.getDeserializer(Text.class);
      valDeserializer = serializationFactory.getDeserializer(IntWritable.class);
      keyDeserializer.open(keyIn);
      valDeserializer.open(valIn);

      while (inMemReader.nextRawKey(keyIn)) {
        inMemReader.nextRawValue(valIn);
      }
    }
    inStream.close();
  }

  @Test(timeout = 5000)
  //Test InMemoryWriter
  public void testInMemoryWriter() throws IOException {
    InMemoryWriter writer = null;
    BoundedByteArrayOutputStream bout = new BoundedByteArrayOutputStream(1024 * 1024);

    List<KVPair> data = KVDataGen.generateTestData(true, 10);

    //No RLE, No RepeatKeys, no compression
    writer = new InMemoryWriter(bout);
    writeTestFileUsingDataBuffer(writer, false, data);
    readUsingInMemoryReader(bout.getBuffer(), data);

    //No RLE, RepeatKeys, no compression
    bout.reset();
    writer = new InMemoryWriter(bout);
    writeTestFileUsingDataBuffer(writer, true, data);
    readUsingInMemoryReader(bout.getBuffer(), data);

    //RLE, No RepeatKeys, no compression
    bout.reset();
    writer = new InMemoryWriter(bout, true);
    writeTestFileUsingDataBuffer(writer, false, data);
    readUsingInMemoryReader(bout.getBuffer(), data);

    //RLE, RepeatKeys, no compression
    bout.reset();
    writer = new InMemoryWriter(bout, true);
    writeTestFileUsingDataBuffer(writer, true, data);
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
  // Basic test
  public void testFileBackedInMemIFileWriter() throws IOException {
    List<KVPair> data = new ArrayList<>();
    List<IntWritable> values = new ArrayList<>();
    Text key = new Text("key");
    IntWritable val = new IntWritable(1);
    for(int i = 0; i < 5; i++) {
      data.add(new KVPair(key, val));
      values.add(val);
    }

    TezTaskOutputFiles tezTaskOutput = new TezTaskOutputFiles(defaultConf, "uniqueId", 1);
    IFile.FileBackedInMemIFileWriter writer = new IFile.FileBackedInMemIFileWriter(defaultConf, localFs, tezTaskOutput,
        Text.class, IntWritable.class, codec, null, null,
        200);

    writer.appendKeyValues(data.get(0).getKey(), values.iterator());
    Text lastKey = new Text("key3");
    IntWritable lastVal = new IntWritable(10);
    data.add(new KVPair(lastKey, lastVal));
    writer.append(lastKey, lastVal);
    writer.close();

    byte[] bytes = new byte[(int) writer.getRawLength()];
    IFile.Reader.readToMemory(bytes,
        new ByteArrayInputStream(ByteString.copyFrom(writer.getData()).toByteArray()),
        (int) writer.getCompressedLength(), codec, false, -1);
    readUsingInMemoryReader(bytes, data);
  }

  @Test(timeout = 5000)
  // Basic test
  public void testFileBackedInMemIFileWriterWithSmallBuffer() throws IOException {
    List<KVPair> data = new ArrayList<>();
    TezTaskOutputFiles tezTaskOutput = new TezTaskOutputFiles(defaultConf, "uniqueId", 1);
    IFile.FileBackedInMemIFileWriter writer = new IFile.FileBackedInMemIFileWriter(defaultConf, localFs, tezTaskOutput,
        Text.class, IntWritable.class, codec, null, null,
        2);

    // empty ifile
    writer.close();

    // Buffer should have self adjusted. So for this empty file, it shouldn't
    // hit disk.
    assertFalse("Data should have been flushed to disk", writer.isDataFlushedToDisk());

    byte[] bytes = new byte[(int) writer.getRawLength()];
    IFile.Reader.readToMemory(bytes,
        new ByteArrayInputStream(ByteString.copyFrom(writer.getData()).toByteArray()),
        (int) writer.getCompressedLength(), codec, false, -1);

    readUsingInMemoryReader(bytes, data);
  }

  @Test(timeout = 20000)
  // Test file spill over scenario
  public void testFileBackedInMemIFileWriter_withSpill() throws IOException {
    List<KVPair> data = new ArrayList<>();
    List<IntWritable> values = new ArrayList<>();

    Text key = new Text("key");
    IntWritable val = new IntWritable(1);
    for(int i = 0; i < 5; i++) {
      data.add(new KVPair(key, val));
      values.add(val);
    }

    // Setting cache limit to 20. Actual data would be around 43 bytes, so it would spill over.
    TezTaskOutputFiles tezTaskOutput = new TezTaskOutputFiles(defaultConf, "uniqueId", 1);
    IFile.FileBackedInMemIFileWriter writer = new IFile.FileBackedInMemIFileWriter(defaultConf, localFs, tezTaskOutput,
        Text.class, IntWritable.class, codec, null, null,
        20);
    writer.setOutputPath(outputPath);

    writer.appendKeyValues(data.get(0).getKey(), values.iterator());
    Text lastKey = new Text("key3");
    IntWritable lastVal = new IntWritable(10);

    data.add(new KVPair(lastKey, lastVal));
    writer.append(lastKey, lastVal);
    writer.close();

    assertTrue("Data should have been flushed to disk", writer.isDataFlushedToDisk());

    // Read output content to memory
    FSDataInputStream inStream = localFs.open(outputPath);
    byte[] bytes = new byte[(int) writer.getRawLength()];

    IFile.Reader.readToMemory(bytes, inStream,
        (int) writer.getCompressedLength(), codec, false, -1);
    inStream.close();

    readUsingInMemoryReader(bytes, data);
  }

  @Test(timeout = 5000)
  // Test empty file case
  public void testEmptyFileBackedInMemIFileWriter() throws IOException {
    List<KVPair> data = new ArrayList<>();
    TezTaskOutputFiles
        tezTaskOutput = new TezTaskOutputFiles(defaultConf, "uniqueId", 1);

    IFile.FileBackedInMemIFileWriter writer = new IFile.FileBackedInMemIFileWriter(defaultConf, localFs, tezTaskOutput,
        Text.class, IntWritable.class, codec, null, null,
        100);

    // empty ifile
    writer.close();

    byte[] bytes = new byte[(int) writer.getRawLength()];

    IFile.Reader.readToMemory(bytes,
        new ByteArrayInputStream(ByteString.copyFrom(writer.getData()).toByteArray()),
        (int) writer.getCompressedLength(), codec, false, -1);

    readUsingInMemoryReader(bytes, data);
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

  @Test(timeout = 20000)
  public void testReadToDisk() throws IOException {
    // verify sending a stream of zeroes generates an error
    byte[] zeroData = new byte[1000];
    Arrays.fill(zeroData, (byte) 0);
    ByteArrayInputStream in = new ByteArrayInputStream(zeroData);
    try {
      IFile.Reader.readToDisk(new ByteArrayOutputStream(), in, zeroData.length, false, 0);
      fail("Exception should have been thrown");
    } catch (IOException e) {
    }

    // verify sending same stream of zeroes with a valid IFile header still
    // generates an error
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(IFile.HEADER);
    baos.write(zeroData);
    try {
      IFile.Reader.readToDisk(new ByteArrayOutputStream(),
          new ByteArrayInputStream(baos.toByteArray()), zeroData.length, false, 0);
      fail("Exception should have been thrown");
    } catch (IOException e) {
      assertTrue(e instanceof ChecksumException);
    }

    // verify valid data is copied properly
    List<KVPair> data = KVDataGen.generateTestData(true, 0);
    Writer writer = writeTestFile(false, false, data, codec);
    baos.reset();
    IFile.Reader.readToDisk(baos, localFs.open(outputPath), writer.getCompressedLength(),
        false, 0);
    byte[] diskData = baos.toByteArray();
    Reader reader = new Reader(new ByteArrayInputStream(diskData), diskData.length,
        codec, null, null, false, 0, 1024);
    verifyData(reader, data);
    reader.close();
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
    writeTestFile(writer, repeatKeys, data);
    out.close();
    return  writer;
  }

  private Writer writeTestFile(IFile.Writer writer, boolean repeatKeys,
      List<KVPair> data) throws IOException {
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
    writeTestFileUsingDataBuffer(writer, repeatKeys, data);
    out.close();
    return writer;
  }

  private Writer writeTestFileUsingDataBuffer(Writer writer, boolean repeatKeys,
      List<KVPair> data) throws IOException {
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
      previousKey.reset(key.getData(), 0, key.getLength());
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
