package org.apache.tez.runtime.library.common;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BoundedByteArrayOutputStream;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.counters.GenericCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.comparator.TezBytesComparator;
import org.apache.tez.runtime.library.common.serializer.TezBytesWritableSerialization;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.InMemoryReader;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.InMemoryWriter;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.MergeManager;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezMerger;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.util.collections.Sets;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@RunWith(Parameterized.class)
public class TestValuesIterator {

  private static final Logger LOG = LoggerFactory.getLogger(TestValuesIterator.class);

  static final String TEZ_BYTES_SERIALIZATION = TezBytesWritableSerialization.class.getName();

  enum TestWithComparator {
    LONG, INT, BYTES, TEZ_BYTES, TEXT, CUSTOM
  }

  Configuration conf;
  FileSystem fs;
  static final Random rnd = new Random();

  final Class keyClass;
  final Class valClass;
  final RawComparator comparator;
  final RawComparator correctComparator;
  final boolean expectedTestResult;

  int mergeFactor;
  //For storing original data
  final ListMultimap<Writable, Writable> originalData;

  TezRawKeyValueIterator rawKeyValueIterator;

  Path baseDir;
  Path tmpDir;
  Path[] streamPaths; //merge stream paths

  /**
   * Constructor
   *
   * @param serializationClassName serialization class to be used
   * @param key                    key class name
   * @param val                    value class name
   * @param comparator             to be used
   * @param correctComparator      (real comparator to be used for correct results)
   * @param testResult             expected result
   * @throws IOException
   */
  public TestValuesIterator(String serializationClassName, Class key, Class val,
      TestWithComparator comparator, TestWithComparator correctComparator, boolean testResult)
      throws IOException {
    this.keyClass = key;
    this.valClass = val;
    this.comparator = getComparator(comparator);
    this.correctComparator =
        (correctComparator == null) ? this.comparator : getComparator(correctComparator);
    this.expectedTestResult = testResult;
    originalData = LinkedListMultimap.create();
    setupConf(serializationClassName);
  }

  private void setupConf(String serializationClassName) throws IOException {
    mergeFactor = 2;
    conf = new Configuration();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_IO_SORT_FACTOR, mergeFactor);
    if (serializationClassName != null) {
      conf.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, serializationClassName + ","
          + conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY));
    }
    baseDir = new Path(".", this.getClass().getName());
    String localDirs = baseDir.toString();
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs);
    fs = FileSystem.getLocal(conf);
  }

  @Before
  public void setup() throws Exception {
    fs.mkdirs(baseDir);
    tmpDir = new Path(baseDir, "tmp");
  }

  @After
  public void cleanup() throws Exception {
    fs.delete(baseDir, true);
    originalData.clear();
  }

  @Test(timeout = 20000)
  public void testIteratorWithInMemoryReader() throws IOException, InterruptedException {
    ValuesIterator iterator = createIterator(true);
    verifyIteratorData(iterator);
  }

  @Test(timeout = 20000)
  public void testIteratorWithIFileReader() throws IOException, InterruptedException {
    ValuesIterator iterator = createIterator(false);
    verifyIteratorData(iterator);
  }

  @Test(timeout = 20000)
  public void testCountedIteratorWithInmemoryReader() throws IOException, InterruptedException {
    verifyCountedIteratorReader(true);
  }

  @Test(timeout = 20000)
  public void testCountedIteratorWithIFileReader() throws IOException, InterruptedException {
    verifyCountedIteratorReader(false);
  }

  private void verifyCountedIteratorReader(boolean inMemory) throws IOException, InterruptedException {
    TezCounter keyCounter = new GenericCounter("inputKeyCounter", "y3");
    TezCounter tupleCounter = new GenericCounter("inputValuesCounter", "y4");
    ValuesIterator iterator = createCountedIterator(inMemory, keyCounter,
        tupleCounter);
    List<Integer> sequence = verifyIteratorData(iterator);
    if (expectedTestResult) {
      assertEquals((long) sequence.size(), keyCounter.getValue());
      long rows = 0;
      for (Integer i : sequence) {
        rows += i.longValue();
      }
      assertEquals(rows, tupleCounter.getValue());
    }
  }

  @Test(timeout = 20000)
  public void testIteratorWithIFileReaderEmptyPartitions() throws IOException, InterruptedException {
    ValuesIterator iterator = createEmptyIterator(false);
    assertTrue(iterator.moveToNext() == false);

    iterator = createEmptyIterator(true);
    assertTrue(iterator.moveToNext() == false);
  }

  private void getNextFromFinishedIterator(ValuesIterator iterator) {
    try {
      boolean hasNext = iterator.moveToNext();
      fail();
    } catch(IOException e) {
      assertTrue(e.getMessage().contains("Please check if you are invoking moveToNext()"));
    }
  }

  private ValuesIterator createEmptyIterator(boolean inMemory)
      throws IOException, InterruptedException {
    if (!inMemory) {
      streamPaths = new Path[0];
      //This will return EmptyIterator
      rawKeyValueIterator =
          TezMerger.merge(conf, fs, keyClass, valClass, null,
              false, -1, 1024, streamPaths, false, mergeFactor, tmpDir, comparator,
              new ProgressReporter(), null, null, null, null);
    } else {
      List<TezMerger.Segment> segments = Lists.newLinkedList();
      //This will return EmptyIterator
      rawKeyValueIterator =
          TezMerger.merge(conf, fs, keyClass, valClass, segments, mergeFactor, tmpDir,
              comparator, new ProgressReporter(), new GenericCounter("readsCounter", "y"),
              new GenericCounter("writesCounter", "y1"),
              new GenericCounter("bytesReadCounter", "y2"), new Progress());
    }
    return new ValuesIterator(rawKeyValueIterator, comparator,
        keyClass, valClass, conf, (TezCounter) new GenericCounter("inputKeyCounter", "y3"),
        (TezCounter) new GenericCounter("inputValueCounter", "y4"));
  }


  /**
   * Tests whether data in valuesIterator matches with sorted input data set.
   * 
   * Returns a list of value counts for each key.
   * 
   * @param valuesIterator
   * @return List
   * @throws IOException
   */
  private List<Integer> verifyIteratorData(
      ValuesIterator valuesIterator) throws IOException {
    boolean result = true;

    ArrayList<Integer> sequence = new ArrayList<Integer>();

    //sort original data based on comparator
    ListMultimap<Writable, Writable> sortedMap =
        new ImmutableListMultimap.Builder<Writable, Writable>()
            .orderKeysBy(this.correctComparator).putAll
            (originalData).build();

    Set<Map.Entry<Writable, Writable>> oriKeySet = Sets.newSet();
    oriKeySet.addAll(sortedMap.entries());

    //Iterate through sorted data and valuesIterator for verification
    for (Map.Entry<Writable, Writable> entry : oriKeySet) {

      assertTrue(valuesIterator.moveToNext());

      Writable oriKey = entry.getKey();
      //Verify if the key and the original key are same
      if (!oriKey.equals((Writable) valuesIterator.getKey())) {
        result = false;
        break;
      }

      int valueCount = 0;
      //Verify values
      Iterator<Writable> vItr = valuesIterator.getValues().iterator();
      for (Writable val : sortedMap.get(oriKey)) {
        assertTrue(vItr.hasNext());

        //Verify if the values are same
        if (!val.equals((Writable) vItr.next())) {
          result = false;
          break;
        }

        valueCount++;
      }
      sequence.add(valueCount);
      assertTrue("At least 1 value per key", valueCount > 0);
    }
    if (expectedTestResult) {
      assertTrue(result);

      assertFalse(valuesIterator.moveToNext());
      getNextFromFinishedIterator(valuesIterator);
    } else {
      while(valuesIterator.moveToNext()) {
        //iterate through all keys
      }
      getNextFromFinishedIterator(valuesIterator);
      assertFalse(result);
    }

    return sequence;
  }

  /**
   * Create sample data (in memory / disk based), merge them and return ValuesIterator
   *
   * @param inMemory
   * @return ValuesIterator
   * @throws IOException
   */
  private ValuesIterator createIterator(boolean inMemory) throws IOException, InterruptedException {
    if (!inMemory) {
      streamPaths = createFiles();
      //Merge all files to get KeyValueIterator
      rawKeyValueIterator =
          TezMerger.merge(conf, fs, keyClass, valClass, null,
              false, -1, 1024, streamPaths, false, mergeFactor, tmpDir, comparator,
              new ProgressReporter(), null, null, null, null);
    } else {
      List<TezMerger.Segment> segments = createInMemStreams();
      rawKeyValueIterator =
          TezMerger.merge(conf, fs, keyClass, valClass, segments, mergeFactor, tmpDir,
              comparator, new ProgressReporter(), new GenericCounter("readsCounter", "y"),
              new GenericCounter("writesCounter", "y1"),
              new GenericCounter("bytesReadCounter", "y2"), new Progress());
    }
    return new ValuesIterator(rawKeyValueIterator, comparator,
        keyClass, valClass, conf, (TezCounter) new GenericCounter("inputKeyCounter", "y3"),
        (TezCounter) new GenericCounter("inputValueCounter", "y4"));
  }

  /**
   * Create sample data (in memory), with an attached counter  and return ValuesIterator
   *
   * @param inMemory
   * @param keyCounter
   * @param tupleCounter
   * @return ValuesIterator
   * @throws IOException
   */
  private ValuesIterator createCountedIterator(boolean inMemory, TezCounter keyCounter, TezCounter tupleCounter)
      throws IOException, InterruptedException {
    if (!inMemory) {
      streamPaths = createFiles();
      //Merge all files to get KeyValueIterator
      rawKeyValueIterator =
          TezMerger.merge(conf, fs, keyClass, valClass, null,
              false, -1, 1024, streamPaths, false, mergeFactor, tmpDir, comparator,
              new ProgressReporter(), null, null, null, null);
    } else {
      List<TezMerger.Segment> segments = createInMemStreams();
      rawKeyValueIterator =
          TezMerger.merge(conf, fs, keyClass, valClass, segments, mergeFactor, tmpDir,
              comparator, new ProgressReporter(), new GenericCounter("readsCounter", "y"),
              new GenericCounter("writesCounter", "y1"),
              new GenericCounter("bytesReadCounter", "y2"), new Progress());
    }
    return new ValuesIterator(rawKeyValueIterator, comparator,
        keyClass, valClass, conf, keyCounter, tupleCounter);
  }

  @Parameterized.Parameters(name = "test[{0}, {1}, {2}, {3} {4} {5} {6}]")
  public static Collection<Object[]> getParameters() {
    Collection<Object[]> parameters = new ArrayList<Object[]>();

    //parameters for constructor
    parameters.add(new Object[]
        { null, Text.class, Text.class, TestWithComparator.TEXT, null, true });
    parameters.add(new Object[]
        { null, LongWritable.class, Text.class, TestWithComparator.LONG, null, true });
    parameters.add(new Object[]
        { null, IntWritable.class, Text.class, TestWithComparator.INT, null, true });
    parameters.add(new Object[]
        { null, BytesWritable.class, BytesWritable.class, TestWithComparator.BYTES, null, true });
    parameters.add(new Object[]
        {
            TEZ_BYTES_SERIALIZATION, BytesWritable.class, BytesWritable.class,
            TestWithComparator.TEZ_BYTES, null, true
        });
    parameters.add(new Object[]
        {
            TEZ_BYTES_SERIALIZATION, BytesWritable.class, LongWritable.class,
            TestWithComparator.TEZ_BYTES,
            null, true
        });
    parameters.add(new Object[]
        {
            TEZ_BYTES_SERIALIZATION, CustomKey.class, LongWritable.class,
            TestWithComparator.TEZ_BYTES,
            null, true
        });

    //negative tests
    parameters.add(new Object[]
        {
            TEZ_BYTES_SERIALIZATION, BytesWritable.class, BytesWritable.class,
            TestWithComparator.BYTES,
            TestWithComparator.TEZ_BYTES, false
        });
    parameters.add(new Object[]
        {
            TEZ_BYTES_SERIALIZATION, CustomKey.class, LongWritable.class, TestWithComparator.CUSTOM,
            TestWithComparator.TEZ_BYTES, false
        });
    return parameters;
  }

  private RawComparator getComparator(TestWithComparator comparator) {
    switch (comparator) {
    case LONG:
      return new LongWritable.Comparator();
    case INT:
      return new IntWritable.Comparator();
    case BYTES:
      return new BytesWritable.Comparator();
    case TEZ_BYTES:
      return new TezBytesComparator();
    case TEXT:
      return new Text.Comparator();
    case CUSTOM:
      return new CustomKey.Comparator();
    default:
      return null;
    }
  }

  private Path[] createFiles() throws IOException {
    int numberOfStreams = Math.max(2, rnd.nextInt(10));
    mergeFactor = Math.max(mergeFactor, numberOfStreams);
    LOG.info("No of streams : " + numberOfStreams);

    Path[] paths = new Path[numberOfStreams];
    for (int i = 0; i < numberOfStreams; i++) {
      paths[i] = new Path(baseDir, "ifile_" + i + ".out");
      FSDataOutputStream out = fs.create(paths[i]);
      //write data with RLE
      IFile.Writer writer = new IFile.Writer(conf, out, keyClass, valClass, null, null, null, true);
      Map<Writable, Writable> data = createData();

      for (Map.Entry<Writable, Writable> entry : data.entrySet()) {
        writer.append(entry.getKey(), entry.getValue());
        originalData.put(entry.getKey(), entry.getValue());
        if (rnd.nextInt() % 2 == 0) {
          for (int j = 0; j < rnd.nextInt(100); j++) {
            //add some duplicate keys
            writer.append(entry.getKey(), entry.getValue());
            originalData.put(entry.getKey(), entry.getValue());
          }
        }
      }
      LOG.info("Wrote " + data.size() + " in " + paths[i]);
      data.clear();
      writer.close();
      out.close();
    }
    return paths;
  }

  /**
   * create inmemory segments
   *
   * @return
   * @throws IOException
   */
  public List<TezMerger.Segment> createInMemStreams() throws IOException {
    int numberOfStreams = Math.max(2, rnd.nextInt(10));
    LOG.info("No of streams : " + numberOfStreams);

    SerializationFactory serializationFactory = new SerializationFactory(conf);
    Serializer keySerializer = serializationFactory.getSerializer(keyClass);
    Serializer valueSerializer = serializationFactory.getSerializer(valClass);

    LocalDirAllocator localDirAllocator =
        new LocalDirAllocator(TezRuntimeFrameworkConfigs.LOCAL_DIRS);
    InputContext context = createTezInputContext();
    MergeManager mergeManager = new MergeManager(conf, fs, localDirAllocator,
        context, null, null, null, null, null, 1024 * 1024 * 10, null, false, -1);

    DataOutputBuffer keyBuf = new DataOutputBuffer();
    DataOutputBuffer valBuf = new DataOutputBuffer();
    DataInputBuffer keyIn = new DataInputBuffer();
    DataInputBuffer valIn = new DataInputBuffer();
    keySerializer.open(keyBuf);
    valueSerializer.open(valBuf);

    List<TezMerger.Segment> segments = new LinkedList<TezMerger.Segment>();
    for (int i = 0; i < numberOfStreams; i++) {
      BoundedByteArrayOutputStream bout = new BoundedByteArrayOutputStream(1024 * 1024);
      InMemoryWriter writer =
          new InMemoryWriter(bout);
      Map<Writable, Writable> data = createData();
      //write data
      for (Map.Entry<Writable, Writable> entry : data.entrySet()) {
        keySerializer.serialize(entry.getKey());
        valueSerializer.serialize(entry.getValue());
        keyIn.reset(keyBuf.getData(), 0, keyBuf.getLength());
        valIn.reset(valBuf.getData(), 0, valBuf.getLength());
        writer.append(keyIn, valIn);
        originalData.put(entry.getKey(), entry.getValue());
        keyBuf.reset();
        valBuf.reset();
        keyIn.reset();
        valIn.reset();
      }
      IFile.Reader reader = new InMemoryReader(mergeManager, null, bout.getBuffer(), 0,
          bout.getBuffer().length);
      segments.add(new TezMerger.Segment(reader, true));

      data.clear();
      writer.close();
    }
    return segments;
  }

  private InputContext createTezInputContext() {
    TezCounters counters = new TezCounters();
    InputContext inputContext = mock(InputContext.class);
    doReturn(1024 * 1024 * 100l).when(inputContext).getTotalMemoryAvailableToTask();
    doReturn(counters).when(inputContext).getCounters();
    doReturn(1).when(inputContext).getInputIndex();
    doReturn("srcVertex").when(inputContext).getSourceVertexName();
    doReturn(1).when(inputContext).getTaskVertexIndex();
    doReturn(UserPayload.create(ByteBuffer.wrap(new byte[1024]))).when(inputContext).getUserPayload();
    return inputContext;
  }

  private Map<Writable, Writable> createData() {
    Map<Writable, Writable> map = new TreeMap<Writable, Writable>(comparator);
    for (int j = 0; j < Math.max(10, rnd.nextInt(50)); j++) {
      Writable key = createData(keyClass);
      Writable value = createData(valClass);
      map.put(key, value);
      //sortedDataMap.put(key, value);
    }
    return map;
  }


  private Writable createData(Class c) {
    if (c.getName().equalsIgnoreCase(BytesWritable.class.getName())) {
      return new BytesWritable(new BigInteger(256, rnd).toString().getBytes());
    } else if (c.getName().equalsIgnoreCase(IntWritable.class.getName())) {
      return new IntWritable(rnd.nextInt());
    } else if (c.getName().equalsIgnoreCase(LongWritable.class.getName())) {
      return new LongWritable(rnd.nextLong());
    } else if (c.getName().equalsIgnoreCase(CustomKey.class.getName())) {
      String rndStr = new BigInteger(256, rnd).toString() + "_" + new BigInteger(256,
          rnd).toString();
      return new CustomKey(rndStr.getBytes(), rndStr.hashCode());
    } else if (c.getName().equalsIgnoreCase(Text.class.getName())) {
      String rndStr = new BigInteger(256, rnd).toString() + "_"
          + new BigInteger(256, rnd).toString();
      return new Text(rndStr);
    } else {
      throw new IllegalArgumentException("Illegal argument : " + c.getName());
    }
  }

  private static class ProgressReporter implements Progressable {
    @Override public void progress() {
      //no impl
    }
  }

  //Custom key and comparator
  public static class CustomKey extends BytesWritable {
    private static final int LENGTH_BYTES = 4;
    private int hashCode;

    public CustomKey() {
    }

    public CustomKey(byte[] data, int hashCode) {
      super(data);
      this.hashCode = hashCode;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    public static class Comparator extends WritableComparator {
      public Comparator() {
        super(CustomKey.class);
      }

      /**
       * Compare the buffers in serialized form.
       */
      @Override
      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return compareBytes(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES, b2, s2
            + LENGTH_BYTES, l2 - LENGTH_BYTES);
      }
    }

    static {
      WritableComparator.define(CustomKey.class, new Comparator());
    }
  }
}
