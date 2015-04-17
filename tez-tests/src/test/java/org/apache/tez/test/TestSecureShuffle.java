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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.tez.mapreduce.examples.TestOrderedWordCount;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test to verify secure-shuffle (SSL mode) in Tez
 */
@RunWith(Parameterized.class)
public class TestSecureShuffle {

  private static MiniDFSCluster miniDFSCluster;
  private static MiniTezCluster miniTezCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem fs;
  private static Path inputLoc = new Path("/tmp/sample.txt");
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestSecureShuffle.class.getName() + "-tmpDir";
  private static File keysStoresDir = new File(TEST_ROOT_DIR, "keystores");

  private boolean enableSSLInCluster; //To set ssl config in cluster
  private int resultWithTezSSL; //expected result with tez ssl setting
  private int resultWithoutTezSSL; //expected result without tez ssl setting

  public TestSecureShuffle(boolean sslInCluster, int resultWithTezSSL, int resultWithoutTezSSL) {
    this.enableSSLInCluster = sslInCluster;
    this.resultWithTezSSL = resultWithTezSSL;
    this.resultWithoutTezSSL = resultWithoutTezSSL;
  }

  @Parameterized.Parameters(name = "test[sslInCluster:{0}, resultWithTezSSL:{1}, resultWithoutTezSSL:{2}]")
  public static Collection<Object[]> getParameters() {
    Collection<Object[]> parameters = new ArrayList<Object[]>();
    //enable ssl in cluster, succeed with tez-ssl enabled, fail with tez-ssl disabled
    parameters.add(new Object[] { true, 0, 1 });

    //Negative testcase
    // disable ssl in cluster, fail with tez-ssl enabled, succeed with tez-ssl disabled
    parameters.add(new Object[] { false, 1, 0 });

    return parameters;
  }

  @BeforeClass
  public static void setupDFSCluster() throws Exception {
    conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_NOEDITLOGCHANNELFLUSH, false);
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
    miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).format(true).build();
    fs = miniDFSCluster.getFileSystem();
    conf.set("fs.defaultFS", fs.getUri().toString());
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, false);
  }

  @AfterClass
  public static void shutdownDFSCluster() {
    if (miniDFSCluster != null) {
      //shutdown
      miniDFSCluster.shutdown();
    }
  }

  @Before
  public void setupTezCluster() throws Exception {
    if (enableSSLInCluster) {
      // Enable SSL debugging
      System.setProperty("javax.net.debug", "all");
      setupKeyStores();
    }
    conf.setBoolean(MRConfig.SHUFFLE_SSL_ENABLED_KEY, enableSSLInCluster);

    // 3 seconds should be good enough in local machine
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT, 3 * 1000);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT, 3 * 1000);
    //set to low value so that it can detect failures quickly
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT, 2);

    miniTezCluster = new MiniTezCluster(TestSecureShuffle.class.getName() + "-" +
        (enableSSLInCluster ? "withssl" : "withoutssl"), 1, 1, 1);

    miniTezCluster.init(conf);
    miniTezCluster.start();
    createSampleFile(inputLoc);
  }

  @After
  public void shutdownTezCluster() throws IOException {
    if (miniTezCluster != null) {
      miniTezCluster.stop();
    }
  }

  private void baseTest(int expectedResult) throws Exception {
    Path outputLoc = new Path("/tmp/outPath_" + System.currentTimeMillis());
    TestOrderedWordCount wordCount = new TestOrderedWordCount();
    wordCount.setConf(new Configuration(miniTezCluster.getConfig()));

    String[] args = new String[] { "-DUSE_MR_CONFIGS=false",
        inputLoc.toString(), outputLoc.toString() };
    assertEquals(expectedResult, wordCount.run(args));
  }

  /**
   * Verify whether shuffle works in mini cluster
   *
   * @throws Exception
   */
  @Test(timeout = 240000)
  public void testSecureShuffle() throws Exception {
    //With tez-ssl setting
    miniTezCluster.getConfig().setBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL, true);
    baseTest(this.resultWithTezSSL);

    //Without tez-ssl setting
    miniTezCluster.getConfig().setBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL, false);
    baseTest(this.resultWithoutTezSSL);
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
    for (int i = 0; i < 10; i++) {
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
  }
}
