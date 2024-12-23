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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFileSystemStatisticUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestFileSystemStatisticUpdater.class);

  private static MiniDFSCluster dfsCluster;

  private static final Configuration CONF = new Configuration();
  private static FileSystem remoteFs;

  private static final String TEST_ROOT_DIR = "target" + Path.SEPARATOR +
      TestFileSystemStatisticUpdater.class.getName() + "-tmpDir";

  @BeforeClass
  public static void beforeClass() throws Exception {
    CONF.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
  }

  @AfterClass
  public static void tearDown() {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  @Before
  public void setup() throws IOException {
    FileSystem.clearStatistics();
    try {
      // tear down the whole cluster before each test to completely get rid of file system statistics
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
      dfsCluster = new MiniDFSCluster.Builder(CONF).numDataNodes(2).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
  }

  @Test
  public void basicTest() throws IOException {
    TezCounters counters = new TezCounters();
    TaskCounterUpdater updater = new TaskCounterUpdater(counters, CONF, "pid");

    DFSTestUtil.writeFile(remoteFs, new Path("/tmp/foo/abc.txt"), "xyz");

    updater.updateCounters();
    LOG.info("Counters (after first update): {}", counters);
    assertCounter(counters, FileSystemCounter.OP_MKDIRS, 0); // DFSTestUtil doesn't call separate mkdirs
    assertCounter(counters, FileSystemCounter.OP_CREATE, 1);
    assertCounter(counters, FileSystemCounter.BYTES_WRITTEN, 3); // "xyz"
    assertCounter(counters, FileSystemCounter.WRITE_OPS, 1);
    assertCounter(counters, FileSystemCounter.OP_GET_FILE_STATUS, 1); // DFSTestUtil calls fs.exists
    assertCounter(counters, FileSystemCounter.OP_CREATE, 1);

    DFSTestUtil.writeFile(remoteFs, new Path("/tmp/foo/abc1.txt"), "xyz");

    updater.updateCounters();
    LOG.info("Counters (after second update): {}", counters);
    assertCounter(counters, FileSystemCounter.OP_CREATE, 2);
    assertCounter(counters, FileSystemCounter.BYTES_WRITTEN, 6); // "xyz" has been written twice
    assertCounter(counters, FileSystemCounter.WRITE_OPS, 2);
    assertCounter(counters, FileSystemCounter.OP_GET_FILE_STATUS, 2); // DFSTestUtil calls fs.exists again
    assertCounter(counters, FileSystemCounter.OP_CREATE, 2);

    // Ensure all numbers are reset
    updater.updateCounters();
    LOG.info("Counters (after third update): {}", counters);
    // counter holds its value after clearStatistics + updateCounters
    assertCounter(counters, FileSystemCounter.OP_CREATE, 2);
  }

  private void assertCounter(TezCounters counters, FileSystemCounter fsCounter, int value) {
    TezCounter counter = counters.findCounter(remoteFs.getScheme(), fsCounter);
    Assert.assertNotNull(counter);
    Assert.assertEquals(value, counter.getValue());
  }
}
