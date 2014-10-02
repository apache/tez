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
package org.apache.tez.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.records.DAGProtos;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class TestTezClientUtils {
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestTezClientUtils.class.getName() + "-tmpDir";
  /**
   * 
   */
  @Test (timeout=5000)
  public void validateSetTezJarLocalResourcesNotDefined() throws Exception {

    TezConfiguration conf = new TezConfiguration(false);
    Credentials credentials = new Credentials();
    try {
      TezClientUtils.setupTezJarsLocalResources(conf, credentials);
      Assert.fail("Expected TezUncheckedException");
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid configuration of tez jars"));
    }
  }

  @Test (timeout=5000)
  public void validateSetTezJarLocalResourcesDefinedButEmpty() throws Exception {
    File emptyDir = new File(TEST_ROOT_DIR, "emptyDir");
    emptyDir.deleteOnExit();
    Assert.assertTrue(emptyDir.mkdirs());
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_LIB_URIS, emptyDir.toURI().toURL().toString());
    Credentials credentials = new Credentials();
    try {
      TezClientUtils.setupTezJarsLocalResources(conf, credentials);
      Assert.fail("Expected TezUncheckedException");
    } catch (TezUncheckedException e) {
      Assert.assertTrue(e.getMessage().contains("No files found in locations"));
    }
  }

  /**
   * 
   */
  @Test(expected=FileNotFoundException.class, timeout=5000)
  public void validateSetTezJarLocalResourcesDefinedNonExistingDirectory() throws Exception {

    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_LIB_URIS, "file:///foo");
    Credentials credentials = new Credentials();
    TezClientUtils.setupTezJarsLocalResources(conf, credentials);
  }

  /**
   *
   */
  @Test (timeout=5000)
  public void validateSetTezJarLocalResourcesDefinedExistingDirectory() throws Exception {
    URL[] cp = ((URLClassLoader)ClassLoader.getSystemClassLoader()).getURLs();
    StringBuffer buffer = new StringBuffer();
    for (URL url : cp) {
      buffer.append(url.toExternalForm());
      buffer.append(",");
    }
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_LIB_URIS, buffer.toString());
    Credentials credentials = new Credentials();
    Map<String, LocalResource> localizedMap = TezClientUtils.setupTezJarsLocalResources(conf, credentials);
    Set<String> resourceNames = localizedMap.keySet();
    for (URL url : cp) {
      File file = FileUtils.toFile(url);
      if (file.isDirectory()){
        String[] firList = file.list();
        for (String fileNme : firList) {
          File innerFile = new File(file, fileNme);
          if (!innerFile.isDirectory()){
            assertTrue(resourceNames.contains(innerFile.getName()));
          }
          // not supporting deep hierarchies 
        }
      }
      else {
        assertTrue(resourceNames.contains(file.getName()));
      }
    }
  }

  /**
   * 
   * @throws Exception
   */
  @Test (timeout=5000)
  public void validateSetTezJarLocalResourcesDefinedExistingDirectoryIgnored() throws Exception {
    URL[] cp = ((URLClassLoader)ClassLoader.getSystemClassLoader()).getURLs();
    StringBuffer buffer = new StringBuffer();
    for (URL url : cp) {
      buffer.append(url.toExternalForm());
      buffer.append(",");
    }
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_LIB_URIS, buffer.toString());
    conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    Credentials credentials = new Credentials();
    Map<String, LocalResource> localizedMap = TezClientUtils.setupTezJarsLocalResources(conf, credentials);
    assertTrue(localizedMap.isEmpty());
  }

  /**
   * 
   * @throws Exception
   */
  @Test (timeout=5000)
  public void validateSetTezJarLocalResourcesDefinedExistingDirectoryIgnoredSetToFalse() throws Exception {
    URL[] cp = ((URLClassLoader)ClassLoader.getSystemClassLoader()).getURLs();
    StringBuffer buffer = new StringBuffer();
    for (URL url : cp) {
      buffer.append(url.toExternalForm());
      buffer.append(",");
    }
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_LIB_URIS, buffer.toString());
    conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, false);
    Credentials credentials = new Credentials();
    Map<String, LocalResource> localizedMap = TezClientUtils.setupTezJarsLocalResources(conf, credentials);
    assertFalse(localizedMap.isEmpty());
  }


  @Test (timeout=5000)
  public void testDefaultMemoryJavaOpts() {
    final double factor = 0.8;
    String origJavaOpts = "-Xmx";
    String javaOpts = TezClientUtils.maybeAddDefaultMemoryJavaOpts(origJavaOpts,
        Resource.newInstance(1000, 1), factor);
    Assert.assertEquals(origJavaOpts, javaOpts);

    origJavaOpts = "";
    javaOpts = TezClientUtils.maybeAddDefaultMemoryJavaOpts(origJavaOpts,
        Resource.newInstance(1000, 1), factor);
    Assert.assertTrue(javaOpts.contains("-Xmx800m"));

    origJavaOpts = "";
    javaOpts = TezClientUtils.maybeAddDefaultMemoryJavaOpts(origJavaOpts,
        Resource.newInstance(1, 1), factor);
    Assert.assertTrue(javaOpts.contains("-Xmx1m"));

    origJavaOpts = "";
    javaOpts = TezClientUtils.maybeAddDefaultMemoryJavaOpts(origJavaOpts,
        Resource.newInstance(-1, 1), factor);
    Assert.assertEquals(origJavaOpts, javaOpts);

    origJavaOpts = "";
    javaOpts = TezClientUtils.maybeAddDefaultMemoryJavaOpts(origJavaOpts,
        Resource.newInstance(355, 1), factor);
    Assert.assertTrue(javaOpts.contains("-Xmx284m"));

    origJavaOpts = " -Xms100m ";
    javaOpts = TezClientUtils.maybeAddDefaultMemoryJavaOpts(origJavaOpts,
        Resource.newInstance(355, 1), factor);
    Assert.assertFalse(javaOpts.contains("-Xmx284m"));
    Assert.assertTrue(javaOpts.contains("-Xms100m"));

    origJavaOpts = "";
    javaOpts = TezClientUtils.maybeAddDefaultMemoryJavaOpts(origJavaOpts,
        Resource.newInstance(355, 1), 0);
    Assert.assertEquals(origJavaOpts, javaOpts);

    origJavaOpts = "";
    javaOpts = TezClientUtils.maybeAddDefaultMemoryJavaOpts(origJavaOpts,
        Resource.newInstance(355, 1), 100);
    Assert.assertEquals(origJavaOpts, javaOpts);
  }

  @Test (timeout=5000)
  public void testDefaultLoggingJavaOpts() {
    String origJavaOpts = null;
    String javaOpts = TezClientUtils.maybeAddDefaultLoggingJavaOpts("FOOBAR", origJavaOpts);
    Assert.assertNotNull(javaOpts);
    Assert.assertTrue(javaOpts.contains("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=FOOBAR")
        && javaOpts.contains(TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE));
  }

  // To run this test case see TestTezCommonUtils::testLocalResourceVisibility
  // We do not have much control over the directory structure, cannot mock as the functions are
  // static and do not want to spin up a minitez cluster just for this.
  public static void testLocalResourceVisibility(DistributedFileSystem remoteFs, Configuration conf)
      throws Exception {

    Path topLevelDir = null;
    try {
      FsPermission publicDirPerms = new FsPermission((short) 0755);   // rwxr-xr-x
      FsPermission privateDirPerms = new FsPermission((short) 0754);  // rwxr-xr--
      FsPermission publicFilePerms = new FsPermission((short) 0554);  // r-xr-xr--
      FsPermission privateFilePerms = new FsPermission((short) 0550); // r-xr-x---

      String fsURI = remoteFs.getUri().toString();

      topLevelDir = new Path(fsURI, "/testLRVisibility");
      Assert.assertTrue(remoteFs.mkdirs(topLevelDir, publicDirPerms));

      Path publicSubDir = new Path(topLevelDir, "public_sub_dir");
      Assert.assertTrue(remoteFs.mkdirs(publicSubDir, publicDirPerms));

      Path privateSubDir = new Path(topLevelDir, "private_sub_dir");
      Assert.assertTrue(remoteFs.mkdirs(privateSubDir, privateDirPerms));

      Path publicFile = new Path(publicSubDir, "public_file");
      Assert.assertTrue(remoteFs.createNewFile(publicFile));
      remoteFs.setPermission(publicFile, publicFilePerms);

      Path privateFile = new Path(publicSubDir, "private_file");
      Assert.assertTrue(remoteFs.createNewFile(privateFile));
      remoteFs.setPermission(privateFile, privateFilePerms);

      Path publicFileInPrivateSubdir = new Path(privateSubDir, "public_file_in_private_subdir");
      Assert.assertTrue(remoteFs.createNewFile(publicFileInPrivateSubdir));
      remoteFs.setPermission(publicFileInPrivateSubdir, publicFilePerms);

      TezConfiguration tezConf = new TezConfiguration(conf);
      String tmpTezLibUris = String.format("%s,%s,%s,%s", topLevelDir, publicSubDir, privateSubDir,
          conf.get(TezConfiguration.TEZ_LIB_URIS, ""));
      tezConf.set(TezConfiguration.TEZ_LIB_URIS, tmpTezLibUris);

      Map<String, LocalResource> lrMap =
          TezClientUtils.setupTezJarsLocalResources(tezConf, new Credentials());

      Assert.assertEquals(publicFile.getName(), LocalResourceVisibility.PUBLIC,
          lrMap.get(publicFile.getName()).getVisibility());

      Assert.assertEquals(privateFile.getName(), LocalResourceVisibility.PRIVATE,
          lrMap.get(privateFile.getName()).getVisibility());

      Assert.assertEquals(publicFileInPrivateSubdir.getName(), LocalResourceVisibility.PRIVATE,
          lrMap.get(publicFileInPrivateSubdir.getName()).getVisibility());

      // test tar.gz
      tezConf = new TezConfiguration(conf);
      Path tarFile = new Path(topLevelDir, "foo.tar.gz");
      Assert.assertTrue(remoteFs.createNewFile(tarFile));

      //public
      remoteFs.setPermission(tarFile, publicFilePerms);
      tezConf.set(TezConfiguration.TEZ_LIB_URIS, tarFile.toString());
      lrMap = TezClientUtils.setupTezJarsLocalResources(tezConf, new Credentials());

      Assert.assertEquals(LocalResourceVisibility.PUBLIC,
          lrMap.get(TezConstants.TEZ_TAR_LR_NAME).getVisibility());

      //private
      remoteFs.setPermission(tarFile, privateFilePerms);
      lrMap = TezClientUtils.setupTezJarsLocalResources(tezConf, new Credentials());
      Assert.assertEquals(LocalResourceVisibility.PRIVATE,
          lrMap.get(TezConstants.TEZ_TAR_LR_NAME).getVisibility());

    } finally {
      if (topLevelDir != null) {
        remoteFs.delete(topLevelDir, true);
      }
    }
  }

  @Test(timeout = 5000)
  public void testConfigurationAllowAll() {
    Configuration srcConf = new Configuration(false);

    Map<String, String> confMap = new HashMap<String, String>();
    confMap.put("ipc.timeout", "2000");
    confMap.put("fs.defaultFS", "testfs:///");
    confMap.put("tez.property", "tezProperty");
    confMap.put("yarn.property", "yarnProperty");

    for (Map.Entry<String, String> entry : confMap.entrySet()) {
      srcConf.set(entry.getKey(), entry.getValue());
    }

    DAGProtos.ConfigurationProto confProto = TezClientUtils.createFinalConfProtoForApp(srcConf);

    for (DAGProtos.PlanKeyValuePair kvPair : confProto.getConfKeyValuesList()) {
      String val = confMap.remove(kvPair.getKey());
      assertNotNull(val);
      assertEquals(val, kvPair.getValue());
    }
    assertTrue(confMap.isEmpty());
  }
}
