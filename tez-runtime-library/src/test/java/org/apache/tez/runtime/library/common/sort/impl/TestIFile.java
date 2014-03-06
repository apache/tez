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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.impl.InMemoryReader;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Reader;
import org.apache.tez.runtime.library.common.sort.impl.IFile.Writer;
import org.apache.tez.runtime.library.testutils.KVDataGen;
import org.apache.tez.runtime.library.testutils.KVDataGen.KVPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestIFile {

  private static final Log LOG = LogFactory.getLog(TestIFile.class);

  private static Configuration defaultConf = new Configuration();
  private static FileSystem localFs = null;
  private static Path workDir = null;

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
  @After
  public void cleanup() throws Exception {
    localFs.delete(workDir, true);
  }

  @Test
  public void testRepeatedKeysInMemReaderNoRLE() throws IOException {
    String outputFileName = "ifile.out";
    Path outputPath = new Path(workDir, outputFileName);
    List<KVPair> data = KVDataGen.generateTestData(true);
    Writer writer = writeTestFile(outputPath, false, data);

    FSDataInputStream inStream =  localFs.open(outputPath);
    byte[] bytes = new byte[(int)writer.getRawLength()];

    readDataToMem(inStream, bytes);
    inStream.close();

    InMemoryReader inMemReader = new InMemoryReader(null, new InputAttemptIdentifier(0, 0), bytes, 0, bytes.length);
    readAndVerify(inMemReader, data);
  }

  @Test
  public void testRepeatedKeysFileReaderNoRLE() throws IOException {
    String outputFileName = "ifile.out";
    Path outputPath = new Path(workDir, outputFileName);
    List<KVPair> data = KVDataGen.generateTestData(true);
    writeTestFile(outputPath, false, data);

    IFile.Reader reader = new IFile.Reader(localFs, outputPath, null, null, null, false, 0, -1);

    readAndVerify(reader, data);
    reader.close();
  }

  @Ignore // TEZ-500
  @Test
  public void testRepeatedKeysInMemReaderRLE() throws IOException {
    String outputFileName = "ifile.out";
    Path outputPath = new Path(workDir, outputFileName);
    List<KVPair> data = KVDataGen.generateTestData(true);
    Writer writer = writeTestFile(outputPath, true, data);

    FSDataInputStream inStream =  localFs.open(outputPath);
    byte[] bytes = new byte[(int)writer.getRawLength()];

    readDataToMem(inStream, bytes);
    inStream.close();


    InMemoryReader inMemReader = new InMemoryReader(null, new InputAttemptIdentifier(0, 0), bytes, 0, bytes.length);
    readAndVerify(inMemReader, data);
  }

  @Ignore // TEZ-500
  @Test
  public void testRepeatedKeysFileReaderRLE() throws IOException {
    String outputFileName = "ifile.out";
    Path outputPath = new Path(workDir, outputFileName);
    List<KVPair> data = KVDataGen.generateTestData(true);
    writeTestFile(outputPath, true, data);

    IFile.Reader reader = new IFile.Reader(localFs, outputPath, null, null, null, false, 0, -1);

    readAndVerify(reader, data);
    reader.close();
  }

  private void readDataToMem(FSDataInputStream inStream, byte[] bytes) throws IOException {
    int toRead = bytes.length;
    int offset = 0;
    while (toRead > 0) {
      int ret = inStream.read(bytes, offset, toRead);
      if (ret < 0) {
        throw new IOException("Premature EOF from inputStream");
      }
      toRead -= ret;
      offset += ret;
    }
    LOG.info("Read: " + bytes.length + " bytes");
  }

  private void readAndVerify(Reader reader, List<KVPair> data)
      throws IOException {
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

  private Writer writeTestFile(Path outputPath, boolean useRle, List<KVPair> data)
      throws IOException {

    IFile.Writer writer = new IFile.Writer(defaultConf, localFs, outputPath,
        Text.class, IntWritable.class, null, null, null);
    writer.setRLE(useRle);

    for (KVPair kvp : data) {
      writer.append(kvp.getKey(), kvp.getvalue());
    }

    writer.close();

    LOG.info("Uncompressed: " + writer.getRawLength());
    LOG.info("CompressedSize: " + writer.getCompressedLength());

    return writer;
  }
}
