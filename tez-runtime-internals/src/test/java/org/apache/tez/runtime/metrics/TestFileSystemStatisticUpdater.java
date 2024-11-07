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

package org.apache.tez.runtime.metrics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFileSystemStatisticUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestFileSystemStatisticUpdater.class);

  private static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;

  private static final String TEST_ROOT_DIR = "target" + Path.SEPARATOR +
      TestFileSystemStatisticUpdater.class.getName() + "-tmpDir";

  @BeforeClass
  public static void setup() throws IOException {
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true).racks(null)
          .build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
  }

  @AfterClass
  public static void tearDown() {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  @Test
  public void basicTest() throws IOException {
    TezCounters counters = new TezCounters();
    TaskCounterUpdater updater = new TaskCounterUpdater(counters, conf, "pid");

    remoteFs.mkdirs(new Path("/tmp/foo/"));
    FSDataOutputStream out = remoteFs.create(new Path("/tmp/foo/abc.txt"));
    out.writeBytes("xyz");
    out.close();

    updater.updateCounters();

    LOG.info("Counters: " + counters);
    TezCounter mkdirCounter = counters.findCounter(remoteFs.getScheme(),
        FileSystemCounter.OP_MKDIRS);
    TezCounter createCounter = counters.findCounter(remoteFs.getScheme(),
        FileSystemCounter.OP_CREATE);
    Assert.assertNotNull(mkdirCounter);
    Assert.assertNotNull(createCounter);
    Assert.assertEquals(1, mkdirCounter.getValue());
    Assert.assertEquals(1, createCounter.getValue());

    FSDataOutputStream out1 = remoteFs.create(new Path("/tmp/foo/abc1.txt"));
    out1.writeBytes("xyz");
    out1.close();

    long oldCreateVal = createCounter.getValue();
    updater.updateCounters();

    LOG.info("Counters: " + counters);
    Assert.assertTrue("Counter not updated, old=" + oldCreateVal
        + ", new=" + createCounter.getValue(), createCounter.getValue() > oldCreateVal);

    oldCreateVal = createCounter.getValue();
    // Ensure all numbers are reset
    remoteFs.clearStatistics();
    updater.updateCounters();
    LOG.info("Counters: " + counters);
    Assert.assertEquals(oldCreateVal, createCounter.getValue());

  }



}
