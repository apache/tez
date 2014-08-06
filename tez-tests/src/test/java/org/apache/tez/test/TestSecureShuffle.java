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
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.tez.mapreduce.examples.OrderedWordCount;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.After;
import org.junit.Before;
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
  private static Path outputLoc = new Path("/tmp/outPath");
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestSecureShuffle.class.getName() + "-tmpDir";
  private static File keysStoresDir = new File(TEST_ROOT_DIR, "keystores");

  private boolean enableSSLInCluster; //To set ssl config in cluster
  private boolean enableSSLInTez; //To set ssl config in Tez
  private int expectedResult;

  public TestSecureShuffle(boolean sslInCluster, boolean sslInTez, int expectedResult) {
    this.enableSSLInCluster = sslInCluster;
    this.enableSSLInTez = sslInTez;
    this.expectedResult = expectedResult;
  }

  @Parameterized.Parameters(name = "test[sslInCluster:{0}, sslInTez:{1}, expectedResult:{2}]")
  public static Collection<Object[]> getParameters() {
    Collection<Object[]> parameters = new ArrayList<Object[]>();
    //enable ssl in cluster and in tez.
    parameters.add(new Object[] { true, true, 0 });
    //disable ssl in cluster and in tez.
    parameters.add(new Object[] { false, false, 0 });

    //Negative test cases

    //enable ssl in cluster, but disable in tez side.
    parameters.add(new Object[] { true, false, 1 });
    //disable ssl config in cluster, but enable from tez side.
    parameters.add(new Object[] { false, true, 1 });

    return parameters;
  }


  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
    if (enableSSLInCluster) {
      // Enable SSL debugging
      System.setProperty("javax.net.debug", "all");
      setupKeyStores();
    }

    miniDFSCluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(3).format(true).build();
    fs = miniDFSCluster.getFileSystem();
    conf.set("fs.defaultFS", fs.getUri().toString());

    // 5 seconds should be good enough in local machine
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT, 5 * 1000);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT, 5 * 1000);
    //set to low value so that it can detect failures quickly
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_FAILURES_LIMIT, 2);

    miniTezCluster =
        new MiniTezCluster(TestSecureShuffle.class.getName(), 3, 3, 1);

    miniTezCluster.init(conf);
    miniTezCluster.start();
    createSampleFile(inputLoc);
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(inputLoc, true);
    fs.delete(outputLoc, true);
    if (miniTezCluster != null) {
      miniTezCluster.stop();
    }
    if (miniDFSCluster != null) {
      miniDFSCluster.shutdown();
    }
  }

  /**
   * Verify whether shuffle works in mini cluster
   *
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testSecureShuffle() throws Exception {
    if (enableSSLInTez) {
      miniTezCluster.getConfig().setBoolean(
          TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL, true);
    }

    OrderedWordCount wordCount = new OrderedWordCount();
    wordCount.setConf(new Configuration(miniTezCluster.getConfig()));

    String[] args = new String[] { inputLoc.toString(), outputLoc.toString() };
    assertEquals(this.expectedResult, wordCount.run(args));

    // cleanup output
    fs.delete(outputLoc, true);
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
