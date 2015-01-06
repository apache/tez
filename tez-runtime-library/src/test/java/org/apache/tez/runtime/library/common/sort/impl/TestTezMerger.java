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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.TreeMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.TestMergeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TestTezMerger {

  private static final Log LOG = LogFactory.getLog(TestTezMerger.class);

  private static Configuration defaultConf = new Configuration();
  private static FileSystem localFs = null;
  private static Path workDir = null;

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
  }

  @AfterClass
  public static void cleanup() throws Exception {
    localFs.delete(workDir, true);
  }


  @Test
  public void testMerge() throws Exception {
    /**
     * test with number of files, keys per file and mergefactor
     */

    //empty file
    merge(1, 0, 1);
    merge(100, 0, 5);

    //small files
    merge(2, 10, 2);
    merge(1, 10, 1);
    merge(5, 10, 3);
    merge(200, 10, 100);

    //bigger files
    merge(5, 100, 5);
    merge(5, 1000, 5);
    merge(5, 1000, 10);
    merge(5, 1000, 100);
  }

  private void merge(int fileCount, int keysPerFile, int mergeFactor) throws Exception {
    List<Path> pathList = createIFiles(fileCount, keysPerFile);

    //Merge datasets
    TezMerger merger = new TezMerger();
    TezRawKeyValueIterator records = merger.merge(defaultConf, localFs, IntWritable.class,
        LongWritable.class, null, false, 0, 1024, pathList.toArray(new Path[pathList.size()]),
        true, mergeFactor, new Path(workDir, "tmp_" + System.nanoTime()),
        ConfigUtils.getIntermediateInputKeyComparator(defaultConf), new Reporter(), null, null,
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
        Assert.assertTrue(verificationDataSet.get(k.get()).size() > 1);
        //Ensure this is same as the previous key we saw
        Assert.assertTrue(pk == k.get());
      } else {
        LOG.info("key=" + k.get() + ", val=" + v.get());
      }
      pk = k.get();

      int keyCount = (dataMap.containsKey(k.get())) ? (dataMap.get(k.get()) + 1) : 1;
      dataMap.put(k.get(), keyCount);
    }

    //Verify if the number of distinct entries is the same in source and the test
    Assert.assertTrue("dataMap=" + dataMap.keySet().size() + ", verificationSet=" +
        verificationDataSet.keySet().size(),
        dataMap.keySet().size() == verificationDataSet.keySet().size());

    //Verify with source data
    for (Integer key : verificationDataSet.keySet()) {
      Assert.assertTrue("Data size for " + key + " not matching with source; dataSize:" + dataMap
              .get(key).intValue() + ", source:" + verificationDataSet.get(key).size(),
          dataMap.get(key).intValue() == verificationDataSet.get(key).size());
    }

    //Verify if every key has the same number of repeated items in the source dataset as well
    for (Map.Entry<Integer, Integer> entry : dataMap.entrySet()) {
      Assert.assertTrue(entry.getKey() + "", verificationDataSet.get(entry.getKey()).size() == entry
          .getValue());
    }

    LOG.info("******************");
  }

  private List<Path> createIFiles(int fileCount, int keysPerFile)
      throws IOException {
    List<Path> pathList = Lists.newLinkedList();
    verificationDataSet.clear();
    Random rnd = new Random();
    for (int i = 0; i < fileCount; i++) {
      int repeatCount = ((i % 2 == 0) && keysPerFile > 0) ? rnd.nextInt(keysPerFile) : 0;
      Path ifilePath = writeIFile(keysPerFile, repeatCount);
      pathList.add(ifilePath);
    }
    return pathList;
  }

  static Path writeIFile(int keysPerFile, int repeatCount) throws IOException {
    TreeMultimap<Integer, Long> dataSet = createDataForIFile(keysPerFile, repeatCount);
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
   * @param keyCount approximate number of keys to be created
   * @param repeatCount number of times a key should be repeated
   * @return
   */
  static TreeMultimap<Integer, Long> createDataForIFile(int keyCount, int repeatCount) {
    TreeMultimap<Integer, Long> dataSet = TreeMultimap.create();
    Random rnd = new Random();
    for (int i = 0; i < keyCount; i++) {
      if (repeatCount > 0 && (rnd.nextInt(keyCount) % 2  == 0)) {
        //repeat this key
        for(int j = 0; j < repeatCount; j++) {
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
