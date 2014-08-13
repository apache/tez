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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.examples.OrderedWordCount;
import org.apache.tez.examples.SimpleSessionExample;
import org.apache.tez.mapreduce.examples.IntersectDataGen;
import org.apache.tez.mapreduce.examples.IntersectExample;
import org.apache.tez.mapreduce.examples.IntersectValidate;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Tests for Tez example jobs
 * 
 */
public class TestTezJobs {

  private static final Log LOG = LogFactory.getLog(TestTezJobs.class);

  protected static MiniTezCluster mrrTezCluster;
  protected static MiniDFSCluster dfsCluster;

  private static Configuration conf = new Configuration();
  private static FileSystem remoteFs;

  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR + TestTezJobs.class.getName()
      + "-tmpDir";

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

    if (mrrTezCluster == null) {
      mrrTezCluster = new MiniTezCluster(TestTezJobs.class.getName(), 1, 1, 1);
      Configuration conf = new Configuration();
      conf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
      mrrTezCluster.init(conf);
      mrrTezCluster.start();
    }

  }

  @AfterClass
  public static void tearDown() {
    if (mrrTezCluster != null) {
      mrrTezCluster.stop();
      mrrTezCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
    // TODO Add cleanup code.
  }

  @Test(timeout = 60000)
  public void testIntersectExample() throws Exception {
    IntersectExample intersectExample = new IntersectExample();
    intersectExample.setConf(new Configuration(mrrTezCluster.getConfig()));
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    Path inPath1 = new Path("/tmp/inPath1");
    Path inPath2 = new Path("/tmp/inPath2");
    Path outPath = new Path("/tmp/outPath");
    remoteFs.mkdirs(inPath1);
    remoteFs.mkdirs(inPath2);
    remoteFs.mkdirs(stagingDirPath);

    Set<String> expectedResult = new HashSet<String>();

    FSDataOutputStream out1 = remoteFs.create(new Path(inPath1, "file"));
    FSDataOutputStream out2 = remoteFs.create(new Path(inPath2, "file"));
    BufferedWriter writer1 = new BufferedWriter(new OutputStreamWriter(out1));
    BufferedWriter writer2 = new BufferedWriter(new OutputStreamWriter(out2));
    for (int i = 0; i < 20; i++) {
      String term = "term" + i;
      writer1.write(term);
      writer1.newLine();
      if (i % 2 == 0) {
        writer2.write(term);
        writer2.newLine();
        expectedResult.add(term);
      }
    }
    writer1.close();
    writer2.close();
    out1.close();
    out2.close();

    String[] args = new String[] {
        "-D" + TezConfiguration.TEZ_AM_STAGING_DIR + "=" + stagingDirPath.toString(),
        inPath1.toString(), inPath2.toString(), "1", outPath.toString() };
    assertEquals(0, intersectExample.run(args));

    FileStatus[] statuses = remoteFs.listStatus(outPath, new PathFilter() {
      public boolean accept(Path p) {
        String name = p.getName();
        return !name.startsWith("_") && !name.startsWith(".");
      }
    });
    assertEquals(1, statuses.length);
    FSDataInputStream inStream = remoteFs.open(statuses[0].getPath());
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
    String line;
    while ((line = reader.readLine()) != null) {
      assertTrue(expectedResult.remove(line));
    }
    reader.close();
    inStream.close();
    assertEquals(0, expectedResult.size());
  }

  @Test(timeout = 120000)
  public void testIntersect2() throws Exception {

    Path testDir = new Path("/tmp/testIntersect2");
    Path stagingDirPath = new Path("/tmp/tez-staging-dir");
    remoteFs.mkdirs(stagingDirPath);
    remoteFs.mkdirs(testDir);

    Path dataPath1 = new Path(testDir, "inPath1");
    Path dataPath2 = new Path(testDir, "inPath2");
    Path expectedOutputPath = new Path(testDir, "expectedOutputPath");
    Path outPath = new Path(testDir, "outPath");

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    TezClient tezSession = null;
    try {
      tezSession = new TezClient("IntersectExampleSession", tezConf);
      tezSession.start();

      IntersectDataGen dataGen = new IntersectDataGen();
      String[] dataGenArgs = new String[] {
          dataPath1.toString(), "1048576", dataPath2.toString(), "524288",
          expectedOutputPath.toString(), "2" };
      assertEquals(0, dataGen.run(tezConf, dataGenArgs, tezSession));

      IntersectExample intersect = new IntersectExample();
      String[] intersectArgs = new String[] {
          dataPath1.toString(), dataPath2.toString(), "2", outPath.toString() };
      assertEquals(0, intersect.run(tezConf, intersectArgs, tezSession));

      IntersectValidate intersectValidate = new IntersectValidate();
      String[] intersectValidateArgs = new String[] {
          expectedOutputPath.toString(), outPath.toString(), "3" };
      assertEquals(0, intersectValidate.run(tezConf, intersectValidateArgs, tezSession));

    } finally {
      if (tezSession != null) {
        tezSession.stop();
      }
    }
  }

  private void generateOrderedWordCountInput(Path inputDir) throws IOException {
    Path dataPath1 = new Path(inputDir, "inPath1");
    Path dataPath2 = new Path(inputDir, "inPath2");

    FSDataOutputStream f1 = null;
    FSDataOutputStream f2 = null;
    try {
      f1 = remoteFs.create(dataPath1);
      f2 = remoteFs.create(dataPath2);

      final String prefix = "a";
      for (int i = 1; i <= 10; ++i) {
        final String word = prefix + "_" + i;
        for (int j = 10; j >= i; --j) {
          LOG.info("Writing " + word + " to input files");
          f1.write(word.getBytes());
          f1.writeChars("\t");
          f2.write(word.getBytes());
          f2.writeChars("\t");
        }
      }
      f1.hsync();
      f2.hsync();
    } finally {
      if (f1 != null) {
        f1.close();
      }
      if (f2 != null) {
        f2.close();
      }
    }
  }

  private void verifyOrderedWordCountOutput(Path resultFile) throws IOException {
    FSDataInputStream inputStream = remoteFs.open(resultFile);
    final String prefix = "a";
    int currentCounter = 10;

    byte[] buffer = new byte[4096];
    int bytesRead = inputStream.read(buffer, 0, 4096);

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer, 0, bytesRead)));

    String line;
    while ((line = reader.readLine()) != null) {
      LOG.info("Line: " + line + ", counter=" + currentCounter);
      int pos = line.indexOf("\t");
      String word = line.substring(0, pos-1);
      Assert.assertEquals(prefix + "_" + currentCounter, word);
      String val = line.substring(pos+1, line.length());
      Assert.assertEquals((long)(11 - currentCounter) * 2, (long)Long.valueOf(val));
      currentCounter--;
    }

    Assert.assertEquals(0, currentCounter);
  }
  
  private void verifyOutput(Path outputDir) throws IOException {
    FileStatus[] fileStatuses = remoteFs.listStatus(outputDir);
    Path resultFile = null;
    boolean foundResult = false;
    boolean foundSuccessFile = false;
    for (FileStatus fileStatus : fileStatuses) {
      if (!fileStatus.isFile()) {
        continue;
      }
      if (fileStatus.getPath().getName().equals("_SUCCESS")) {
        foundSuccessFile = true;
        continue;
      }
      if (fileStatus.getPath().getName().startsWith("part-")) {
        if (foundResult) {
          fail("Found 2 part files instead of 1"
              + ", paths=" + resultFile + "," + fileStatus.getPath());
        }
        foundResult = true;
        resultFile = fileStatus.getPath();
        LOG.info("Found output at " + resultFile);
      }
    }
    assertTrue(foundResult);
    assertTrue(resultFile != null);
    assertTrue(foundSuccessFile);
    verifyOrderedWordCountOutput(resultFile);
  }
  
  @Test(timeout = 60000)
  public void testOrderedWordCount() throws Exception {
    String inputDirStr = "/tmp/owc-input/";
    Path inputDir = new Path(inputDirStr);
    Path stagingDirPath = new Path("/tmp/owc-staging-dir");
    remoteFs.mkdirs(inputDir);
    remoteFs.mkdirs(stagingDirPath);
    generateOrderedWordCountInput(inputDir);

    String outputDirStr = "/tmp/owc-output/";
    Path outputDir = new Path(outputDirStr);

    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    TezClient tezSession = null;

    try {

      OrderedWordCount job = new OrderedWordCount();
      Assert.assertTrue("OrderedWordCount failed", job.run(inputDirStr, outputDirStr, tezConf, 2));
      verifyOutput(outputDir);

    } finally {
      remoteFs.delete(stagingDirPath, true);
      if (tezSession != null) {
        tezSession.stop();
      }
    }

  }
  
  @Test(timeout = 60000)
  public void testSimpleSessionExample() throws Exception {
    Path stagingDirPath = new Path("/tmp/owc-staging-dir");
    remoteFs.mkdirs(stagingDirPath);

    int numIterations = 2;
    String[] inputPaths = new String[numIterations];
    String[] outputPaths = new String[numIterations];
    Path[] outputDirs = new Path[numIterations];
    for (int i=0; i<numIterations; ++i) {
      String inputDirStr = "/tmp/owc-input-" + i + "/";
      inputPaths[i] = inputDirStr;
      Path inputDir = new Path(inputDirStr);
      remoteFs.mkdirs(inputDir);
      generateOrderedWordCountInput(inputDir);
      String outputDirStr = "/tmp/owc-output-" + i + "/"; 
      outputPaths[i] = outputDirStr;
      Path outputDir = new Path(outputDirStr);
      outputDirs[i] = outputDir;
    }


    TezConfiguration tezConf = new TezConfiguration(mrrTezCluster.getConfig());
    tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR, stagingDirPath.toString());
    YarnClient yarnClient = YarnClient.createYarnClient();

    try {
      
      yarnClient.init(mrrTezCluster.getConfig());
      yarnClient.start();
      
      List<ApplicationReport> apps = yarnClient.getApplications();
      int appsBeforeCount = apps != null ? apps.size() : 0;

      SimpleSessionExample job = new SimpleSessionExample();
      Assert.assertTrue("SimpleSessionExample failed", job.run(inputPaths, outputPaths, tezConf, 2));

      for (int i=0; i<numIterations; ++i) {
        verifyOutput(outputDirs[i]);
      }
      
      apps = yarnClient.getApplications();
      int appsAfterCount = apps != null ? apps.size() : 0;
      
      // Running in session mode. So should only create 1 more app.
      Assert.assertEquals(appsBeforeCount + 1, appsAfterCount);
    } finally {
      remoteFs.delete(stagingDirPath, true);
      if (yarnClient != null) {
        yarnClient.stop();
      }
    }

  }


}
