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

package org.apache.tez.runtime.library.common.sort.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.TestMergeManager;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class TestTezMerger {

  private static final Logger LOG = LoggerFactory.getLogger(TestTezMerger.class);

  private static Configuration defaultConf = new Configuration();
  private static FileSystem localFs = null;
  private static Path workDir = null;
  private static RawComparator comparator = null;
  private static Random rnd = new Random();

  private static final String SAME_KEY = "SAME_KEY";
  private static final String DIFF_KEY = "DIFF_KEY";

  //store the generated data for final verification
  private static ListMultimap<Integer, Long> verificationDataSet = LinkedListMultimap.create();

  static {
    defaultConf.set("fs.defaultFS", "file:///");
    try {
      localFs = FileSystem.getLocal(defaultConf);
      workDir = new Path(
          new Path(System.getProperty("test.build.data", "/tmp")), TestTezMerger.class.getName())
          .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
      LOG.info("Using workDir: " + workDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    defaultConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, IntWritable.class.getName());
    defaultConf.set(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS, LongWritable.class.getName());
    Path baseDir = new Path(workDir, TestMergeManager.class.getName());
    String localDirs = baseDir.toString();
    defaultConf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS, localDirs);
    comparator = ConfigUtils.getIntermediateInputKeyComparator(defaultConf);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    localFs.delete(workDir, true);
  }

  @Test(timeout = 80000)
  public void testMerge() throws Exception {
    /**
     * test with number of files, keys per file and mergefactor
     */

    //empty file
    merge(1, 0, 1);
    merge(100, 0, 5);

    //small files
    merge(12, 4, 2);
    merge(2, 10, 2);
    merge(1, 10, 1);
    merge(5, 10, 3);
    merge(200, 10, 100);

    //bigger files
    merge(5, 100, 5);
    merge(5, 1000, 5);
    merge(5, 1000, 10);
    merge(5, 1000, 100);

    //Create random mix of files (empty files + files with keys)
    List<Path> pathList = new LinkedList<Path>();
    pathList.clear();
    pathList.addAll(createIFiles(Math.max(2, rnd.nextInt(20)), 0));
    pathList.addAll(createIFiles(Math.max(2, rnd.nextInt(20)), Math.max(2, rnd.nextInt(10))));
    merge(pathList, Math.max(2, rnd.nextInt(10)));
  }

  private Path createIFileWithTextData(List<String> data) throws IOException {
    Path path = new Path(workDir + "/src", "data_" + System.nanoTime() + ".out");
    FSDataOutputStream out = localFs.create(path);
    IFile.Writer writer = new IFile.Writer(defaultConf, out, Text.class,
        Text.class, null, null, null, true);
    for (String key : data) {
      writer.append(new Text(key), new Text(key + "_" + System.nanoTime()));
    }
    writer.close();
    out.close();
    return path;
  }

  /**
   * Verify if the records are as per the expected data set
   *
   * @param records
   * @param expectedResult
   * @throws IOException
   */
  private void verify(TezRawKeyValueIterator records, String[][] expectedResult)
      throws IOException {
    //Iterate through merged dataset (shouldn't throw any exceptions)
    int i = 0;
    while (records.next()) {
      DataInputBuffer key = records.getKey();
      DataInputBuffer value = records.getValue();

      Text k = new Text();
      k.readFields(key);
      Text v = new Text();
      v.readFields(value);

      assertTrue(k.toString().equals(expectedResult[i][0]));

      String correctResult = expectedResult[i][1];

      if (records.isSameKey()) {
        assertTrue("Expected " + correctResult, correctResult.equalsIgnoreCase(SAME_KEY));
        LOG.info("\tSame Key : key=" + k + ", val=" + v);
      } else {
        assertTrue("Expected " + correctResult, correctResult.equalsIgnoreCase(DIFF_KEY));
        LOG.info("key=" + k + ", val=" + v);
      }

      i++;
    }
  }

  @Test(timeout = 5000)
  public void testWithCustomComparator_WithEmptyStrings() throws Exception {
    List<Path> pathList = new LinkedList<Path>();
    List<String> data = Lists.newLinkedList();
    //Merge datasets with custom comparator
    RawComparator rc = new CustomComparator();

    LOG.info("Test with custom comparator with empty strings in middle");

    //Test with 4 files, where some texts are empty strings
    data.add("0");
    data.add("0");
    pathList.add(createIFileWithTextData(data));

    //Second file with empty key
    data.clear();
    data.add("");
    pathList.add(createIFileWithTextData(data));

    //Third file
    data.clear();
    data.add("0");
    data.add("0");
    pathList.add(createIFileWithTextData(data));

    //Third file
    data.clear();
    data.add("1");
    data.add("2");
    pathList.add(createIFileWithTextData(data));

    TezRawKeyValueIterator records = merge(pathList, rc);

    String[][] expectedResult =
        {
            //formatting intentionally
            { "", DIFF_KEY },
            { "0", DIFF_KEY },
              { "0", SAME_KEY },
              { "0", SAME_KEY },
              { "0", SAME_KEY },
            { "1", DIFF_KEY },
            { "2", DIFF_KEY }
        };

    verify(records, expectedResult);
    pathList.clear();
    data.clear();
  }

  @Test(timeout = 5000)
  public void testWithCustomComparator_No_RLE() throws Exception {
    List<Path> pathList = new LinkedList<Path>();
    List<String> data = Lists.newLinkedList();
    //Merge datasets with custom comparator
    RawComparator rc = new CustomComparator();

    LOG.info("Test with custom comparator with no RLE");

    //Test with 3 files,
    data.add("1");
    data.add("4");
    data.add("5");
    pathList.add(createIFileWithTextData(data));

    //Second file with empty key
    data.clear();
    data.add("2");
    data.add("6");
    data.add("7");
    pathList.add(createIFileWithTextData(data));

    //Third file
    data.clear();
    data.add("3");
    data.add("8");
    data.add("9");
    pathList.add(createIFileWithTextData(data));

    TezRawKeyValueIterator records = merge(pathList, rc);

    String[][] expectedResult =
        {
            { "1", DIFF_KEY },
            { "2", DIFF_KEY },
            { "3", DIFF_KEY },
            { "4", DIFF_KEY },
            { "5", DIFF_KEY },
            { "6", DIFF_KEY },
            { "7", DIFF_KEY },
            { "8", DIFF_KEY },
            { "9", DIFF_KEY }
        };

    verify(records, expectedResult);
    pathList.clear();
    data.clear();
  }

  @Test(timeout = 5000)
  public void testWithCustomComparator_RLE_acrossFiles() throws Exception {
    List<Path> pathList = new LinkedList<Path>();
    List<String> data = Lists.newLinkedList();

    LOG.info("Test with custom comparator with RLE spanning across segment boundaries");

    //Test with 2 files, where the RLE keys can span across files
    //First file
    data.clear();
    data.add("0");
    data.add("0");
    pathList.add(createIFileWithTextData(data));

    //Second file
    data.clear();
    data.add("0");
    data.add("1");
    pathList.add(createIFileWithTextData(data));

    //Merge datasets with custom comparator
    RawComparator rc = new CustomComparator();
    TezRawKeyValueIterator records = merge(pathList, rc);

    //expected result
    String[][] expectedResult =
        {
            //formatting intentionally
            { "0", DIFF_KEY },
              { "0", SAME_KEY },
              { "0", SAME_KEY },
            { "1", DIFF_KEY }
        };

    verify(records, expectedResult);
    pathList.clear();
    data.clear();

  }

  @Test(timeout = 5000)
  public void testWithCustomComparator_mixedFiles() throws Exception {
    List<Path> pathList = new LinkedList<Path>();
    List<String> data = Lists.newLinkedList();

    LOG.info("Test with custom comparator with mixed set of segments (empty, non-empty etc)");

    //Test with 2 files, where the RLE keys can span across files
    //First file
    data.clear();
    data.add("0");
    pathList.add(createIFileWithTextData(data));

    //Second file; empty file
    data.clear();
    pathList.add(createIFileWithTextData(data));

    //Third file with empty key
    data.clear();
    data.add("");
    pathList.add(createIFileWithTextData(data));

    //Fourth file with repeated keys
    data.clear();
    data.add("0");
    data.add("0");
    data.add("0");
    pathList.add(createIFileWithTextData(data));

    //Merge datasets with custom comparator
    RawComparator rc = new CustomComparator();
    TezRawKeyValueIterator records = merge(pathList, rc);

    //expected result
    String[][] expectedResult =
        {
            //formatting intentionally
            { "", DIFF_KEY },
            { "0", DIFF_KEY },
              { "0", SAME_KEY },
              { "0", SAME_KEY },
              { "0", SAME_KEY }
        };

    verify(records, expectedResult);
    pathList.clear();
    data.clear();
  }

  @Test(timeout = 5000)
  public void testWithCustomComparator_RLE() throws Exception {
    List<Path> pathList = new LinkedList<Path>();
    List<String> data = Lists.newLinkedList();

    LOG.info("Test with custom comparator 2 files one containing RLE and also other segment "
        + "starting with same key");

    //Test with 2 files, same keys in middle of file
    //First file
    data.clear();
    data.add("1");
    data.add("2");
    data.add("2");
    pathList.add(createIFileWithTextData(data));

    //Second file
    data.clear();
    data.add("2");
    data.add("3");
    pathList.add(createIFileWithTextData(data));

    TezRawKeyValueIterator records = merge(pathList, new CustomComparator());

    String[][] expectedResult =
        {
            //formatting intentionally
            { "1", DIFF_KEY },
            { "2", DIFF_KEY },
              { "2", SAME_KEY },
              { "2", SAME_KEY },
            { "3", DIFF_KEY }
        };

    verify(records, expectedResult);
    pathList.clear();
    data.clear();
  }

  @Test(timeout = 5000)
  public void testWithCustomComparator_RLE2() throws Exception {
    List<Path> pathList = new LinkedList<Path>();
    List<String> data = Lists.newLinkedList();

    LOG.info(
        "Test with custom comparator 3 files with RLE (starting keys) spanning across boundaries");

    //Test with 3 files, same keys in middle of file
    //First file
    data.clear();
    data.add("0");
    data.add("1");
    data.add("1");
    pathList.add(createIFileWithTextData(data));

    //Second file
    data.clear();
    data.add("0");
    data.add("1");
    pathList.add(createIFileWithTextData(data));

    //Third file
    data.clear();
    data.add("0");
    data.add("1");
    data.add("1");
    pathList.add(createIFileWithTextData(data));

    TezRawKeyValueIterator records = merge(pathList, new CustomComparator());
    String[][] expectedResult =
        {
            //formatting intentionally
            { "0", DIFF_KEY },
              { "0", SAME_KEY },
              { "0", SAME_KEY },
            { "1", DIFF_KEY },
              { "1", SAME_KEY },
              { "1", SAME_KEY },
              { "1", SAME_KEY },
              { "1", SAME_KEY }

        };

    verify(records, expectedResult);
    pathList.clear();
    data.clear();
  }

  @Test(timeout = 5000)
  public void testWithCustomComparator() throws Exception {
    List<Path> pathList = new LinkedList<Path>();
    List<String> data = Lists.newLinkedList();

    LOG.info(
        "Test with custom comparator 3 files with RLE (starting keys) spanning across boundaries");

    //Test with 3 files
    //First file
    data.clear();
    data.add("0");
    pathList.add(createIFileWithTextData(data));

    //Second file
    data.clear();
    data.add("0");
    pathList.add(createIFileWithTextData(data));

    //Third file
    data.clear();
    data.add("1");
    pathList.add(createIFileWithTextData(data));

    TezRawKeyValueIterator records = merge(pathList, new CustomComparator());
    String[][] expectedResult =
        {
            //formatting intentionally
            { "0", DIFF_KEY },
              { "0", SAME_KEY },
            { "1", DIFF_KEY }
        };

    verify(records, expectedResult);
    pathList.clear();
    data.clear();
  }

  @Test(timeout = 5000)
  public void testWithCustomComparator_RLE3() throws Exception {
    List<Path> pathList = new LinkedList<Path>();
    List<String> data = Lists.newLinkedList();

    LOG.info("Test with custom comparator");

    //Test with 3 files, same keys in middle of file
    //First file
    data.clear();
    data.add("0");
    pathList.add(createIFileWithTextData(data));

    //Second file
    data.clear();
    data.add("0");
    data.add("1");
    data.add("1");
    pathList.add(createIFileWithTextData(data));

    TezRawKeyValueIterator records = merge(pathList, new CustomComparator());

    String[][] expectedResult =
        {
            //formatting intentionally
            { "0", DIFF_KEY },
              { "0", SAME_KEY },
            { "1", DIFF_KEY },
              { "1", SAME_KEY } };

    verify(records, expectedResult);
    pathList.clear();
    data.clear();
  }

  @Test(timeout = 5000)
  public void testWithCustomComparator_allEmptyFiles() throws Exception {
    List<Path> pathList = new LinkedList<Path>();
    List<String> data = Lists.newLinkedList();

    LOG.info("Test with custom comparator where all files are empty");

    //First file
    pathList.add(createIFileWithTextData(data));

    //Second file
    pathList.add(createIFileWithTextData(data));

    //Third file
    pathList.add(createIFileWithTextData(data));

    //Fourth file
    pathList.add(createIFileWithTextData(data));

    TezRawKeyValueIterator records = merge(pathList, new CustomComparator());

    String[][] expectedResult = new String[0][0];

    verify(records, expectedResult);
  }

  /**
   * Merge the data sets
   *
   * @param pathList
   * @param rc
   * @return
   * @throws IOException
   */
  private TezRawKeyValueIterator merge(List<Path> pathList, RawComparator rc)
      throws IOException, InterruptedException {
    TezMerger merger = new TezMerger();
    TezRawKeyValueIterator records = merger.merge(defaultConf, localFs, IntWritable.class,
        LongWritable.class, null, false, 0, 1024, pathList.toArray(new Path[pathList.size()]),
        true, 4, new Path(workDir, "tmp_" + System.nanoTime()), ((rc == null) ? comparator : rc),
        new Reporter(), null, null,
        null, new Progress());
    return records;
  }



  //Sample comparator to test TEZ-1999 corner case
  static class CustomComparator extends WritableComparator {
    @Override
    //Not a valid comparison, but just to check byte boundaries
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      Preconditions.checkArgument(l2 > 0 && l1 > 0, "l2=" + l2 + ",l1=" + l1);
      ByteBuffer bb1 = ByteBuffer.wrap(b1, s1, l1);
      ByteBuffer bb2 = ByteBuffer.wrap(b2, s2, l2);
      return bb1.compareTo(bb2);
    }
  }

  private void merge(List<Path> pathList, int mergeFactor) throws Exception {
    merge(pathList, mergeFactor, null);
  }

  private void merge(int fileCount, int keysPerFile, int mergeFactor) throws Exception {
    List<Path> pathList = createIFiles(fileCount, keysPerFile);
    merge(pathList, mergeFactor, null);
  }

  private void merge(List<Path> pathList, int mergeFactor, RawComparator rc) throws Exception {
    //Merge datasets
    TezMerger merger = new TezMerger();
    TezRawKeyValueIterator records = merger.merge(defaultConf, localFs, IntWritable.class,
        LongWritable.class, null, false, 0, 1024, pathList.toArray(new Path[pathList.size()]),
        true, mergeFactor, new Path(workDir, "tmp_" + System.nanoTime()),
        ((rc == null) ? comparator : rc), new Reporter(), null, null,
        null,
        new Progress());

    //Verify the merged data is correct
    Map<Integer, Integer> dataMap = Maps.newHashMap();
    int pk = -1;
    while (records.next()) {
      DataInputBuffer key = records.getKey();
      DataInputBuffer value = records.getValue();

      IntWritable k = new IntWritable();
      k.readFields(key);
      LongWritable v = new LongWritable();
      v.readFields(value);

      if (records.isSameKey()) {
        LOG.info("\tSame Key : key=" + k.get() + ", val=" + v.get());
        //More than one key should be present in the source data
        assertTrue(verificationDataSet.get(k.get()).size() > 1);
        //Ensure this is same as the previous key we saw
        assertTrue("previousKey=" + pk + ", current=" + k.get(), pk == k.get());
      } else {
        LOG.info("key=" + k.get() + ", val=" + v.get());
      }
      pk = k.get();

      int keyCount = (dataMap.containsKey(k.get())) ? (dataMap.get(k.get()) + 1) : 1;
      dataMap.put(k.get(), keyCount);
    }

    //Verify if the number of distinct entries is the same in source and the test
    assertTrue("dataMap=" + dataMap.keySet().size() + ", verificationSet=" +
            verificationDataSet.keySet().size(),
        dataMap.keySet().size() == verificationDataSet.keySet().size());

    //Verify with source data
    for (Integer key : verificationDataSet.keySet()) {
      assertTrue("Data size for " + key + " not matching with source; dataSize:" + dataMap
              .get(key).intValue() + ", source:" + verificationDataSet.get(key).size(),
          dataMap.get(key).intValue() == verificationDataSet.get(key).size());
    }

    //Verify if every key has the same number of repeated items in the source dataset as well
    for (Map.Entry<Integer, Integer> entry : dataMap.entrySet()) {
      assertTrue(entry.getKey() + "", verificationDataSet.get(entry.getKey()).size() == entry
          .getValue());
    }

    LOG.info("******************");
    verificationDataSet.clear();
  }

  private List<Path> createIFiles(int fileCount, int keysPerFile)
      throws IOException {
    List<Path> pathList = Lists.newLinkedList();
    Random rnd = new Random();
    for (int i = 0; i < fileCount; i++) {
      int repeatCount = ((i % 2 == 0) && keysPerFile > 0) ? rnd.nextInt(keysPerFile) : 0;
      Path ifilePath = writeIFile(keysPerFile, repeatCount);
      pathList.add(ifilePath);
    }
    return pathList;
  }

  static Path writeIFile(int keysPerFile, int repeatCount) throws
      IOException {
    TreeMultimap<Integer, Long> dataSet = createDataForIFile(keysPerFile, repeatCount);
    LOG.info("DataSet size : " + dataSet.size());
    Path path = new Path(workDir + "/src", "data_" + System.nanoTime() + ".out");
    FSDataOutputStream out = localFs.create(path);
    //create IFile with RLE
    IFile.Writer writer = new IFile.Writer(defaultConf, out, IntWritable.class
        , LongWritable.class, null, null, null, true);

    for (Integer key : dataSet.keySet()) {
      for (Long value : dataSet.get(key)) {
        writer.append(new IntWritable(key), new LongWritable(value));
        verificationDataSet.put(key, value);
      }
    }
    writer.close();
    out.close();
    return path;
  }

  /**
   * Generate data set for ifile.  Create repeated keys if needed.
   *
   * @param keyCount    approximate number of keys to be created
   * @param repeatCount number of times a key should be repeated
   * @return
   */
  static TreeMultimap<Integer, Long> createDataForIFile(int keyCount, int repeatCount) {
    TreeMultimap<Integer, Long> dataSet = TreeMultimap.create();
    Random rnd = new Random();
    for (int i = 0; i < keyCount; i++) {
      if (repeatCount > 0 && (rnd.nextInt(keyCount) % 2 == 0)) {
        //repeat this key
        for (int j = 0; j < repeatCount; j++) {
          IntWritable key = new IntWritable(rnd.nextInt(keyCount));
          LongWritable value = new LongWritable(System.nanoTime());
          dataSet.put(key.get(), value.get());
        }
        i += repeatCount;
        LOG.info("Repeated key count=" + (repeatCount));
      } else {
        IntWritable key = new IntWritable(rnd.nextInt(keyCount));
        LongWritable value = new LongWritable(System.nanoTime());
        dataSet.put(key.get(), value.get());
      }
    }
    for (Integer key : dataSet.keySet()) {
      for (Long value : dataSet.get(key)) {
        LOG.info("Key=" + key + ", val=" + value);
      }
    }
    LOG.info("=============");
    return dataSet;
  }

  private static class Reporter implements Progressable {
    @Override
    public void progress() {
    }
  }

}
