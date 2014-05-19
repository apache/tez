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

package org.apache.tez.test;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.mapreduce.examples.OrderedWordCount;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test to verify secure-shuffle (SSL mode) in Tez
 */
public class TestSecureShuffle {

  private static MiniDFSCluster miniDFSCluster;
  private static MiniTezCluster miniTezCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem fs;
  private static Path inputLoc = new Path("/tmp/sample.txt");
  private static Path outputLoc = new Path("/tmp/outPath");
  private static File keysStoresDir = new File("target/keystores");

  @BeforeClass
  public static void setup() throws Exception {
    // Enable SSL debugging
    System.setProperty("javax.net.debug", "all");
    conf = new Configuration();
    setupKeyStores();

    miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).format(true).build();
    fs = miniDFSCluster.getFileSystem();
    conf.set("fs.defaultFS", fs.getUri().toString());

    // 15 seconds should be good enough in local machine
    conf.setInt(TezJobConfig.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT, 15 * 1000);
    conf.setInt(TezJobConfig.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT, 15 * 1000);

    miniTezCluster =
        new MiniTezCluster(TestSecureShuffle.class.getName(), 3, 3, 1);

    miniTezCluster.init(conf);
    miniTezCluster.start();
    createSampleFile(inputLoc);
  }

  /**
   * Verify whether shuffle works on SSL mode.
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testSecureShuffle() throws Exception {
    miniTezCluster.getConfig().setBoolean(
      TezJobConfig.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL, true);

    OrderedWordCount wordCount = new OrderedWordCount();
    wordCount.setConf(new Configuration(miniTezCluster.getConfig()));

    String[] args = new String[] { inputLoc.toString(), outputLoc.toString() };
    assertEquals(0, wordCount.run(args));

    // cleanup output
    fs.delete(outputLoc, true);
  }

  @AfterClass
  public static void tearDown() {
    if (miniTezCluster != null) {
      miniTezCluster.stop();
    }
    if (miniDFSCluster != null) {
      miniDFSCluster.shutdown();
    }
  }

  /**
   * Create sample file for wordcount program
   *
   * @param inputLoc
   * @throws IOException
   */
  private static void createSampleFile(Path inputLoc) throws IOException {
    fs.deleteOnExit(inputLoc);
    FSDataOutputStream out = fs.create(inputLoc);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    for (int i = 0; i < 50; i++) {
      writer.write("Hello World");
      writer.write("Some other line");
      writer.newLine();
    }
    writer.close();
  }

  /**
   * Create relevant keystores for test cluster
   *
   * @throws Exception
   */
  private static void setupKeyStores() throws Exception {
    keysStoresDir.mkdirs();
    String sslConfsDir =
        KeyStoreTestUtil.getClasspathDir(TestSecureShuffle.class);

    KeyStoreTestUtil.setupSSLConfig(keysStoresDir.getAbsolutePath(),
      sslConfsDir, conf, true);
    conf.setBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY, true);
  }
}
