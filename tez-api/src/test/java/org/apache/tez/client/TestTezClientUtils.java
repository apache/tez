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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
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

    TezConfiguration conf = new TezConfiguration();
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
    Path emptyDirPath = new Path(emptyDir.getAbsolutePath());
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_LIB_URIS, "file://" + emptyDirPath.toString());
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
    Assert.assertTrue(javaOpts.contains("-D" + TezConfiguration.TEZ_ROOT_LOGGER_NAME + "=FOOBAR")
        && javaOpts.contains(TezConfiguration.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE));
  }

  @Ignore @Test (timeout = 5000)
  public void testLocalResourceVisibility() throws Exception {

    Assume.assumeFalse(Shell.WINDOWS);

    File topLevelDir = null;
    File subDir = null;
    File publicFile = null;
    File privateFile = null;
    File subDirFile = null;
    try {
      topLevelDir = createDirectoryOrFile(new File(TEST_ROOT_DIR), "testLRVisibility", true);
      subDir = createDirectoryOrFile(topLevelDir, "subDir", true);
      publicFile = createDirectoryOrFile(topLevelDir, "publicFile", false);
      privateFile = createDirectoryOrFile(topLevelDir, "privateFile", false);
      subDirFile = createDirectoryOrFile(subDir, "subDirFile", false);

      // no direct way to remove execute permission for others, set all to no execute and then
      // only the owner to execute. this does not work well on windows
      Assert.assertTrue(subDir.setExecutable(false, false));
      Assert.assertTrue(subDir.setExecutable(true));

      Assert.assertTrue(privateFile.setReadable(false, false));
      Assert.assertTrue(privateFile.setReadable(true));

      String tezLibUris = topLevelDir.toURI().toString() + "," + subDir.toURI().toString();

      TezConfiguration conf = new TezConfiguration();
      conf.set(TezConfiguration.TEZ_LIB_URIS, tezLibUris);
      Credentials credentials = new Credentials();

      Map<String, LocalResource> localResourceMap = TezClientUtils.setupTezJarsLocalResources(conf,
          credentials);

      Assert.assertTrue(localResourceMap.get(publicFile.getName()).getVisibility() ==
          LocalResourceVisibility.PUBLIC);

      // ancestor directories have execute perms but file does not have read perms for other users.
      Assert.assertTrue(localResourceMap.get(privateFile.getName()).getVisibility() ==
          LocalResourceVisibility.PRIVATE);

      // file has read perms but parent dir does not have execute perms for other users
      Assert.assertTrue(localResourceMap.get(subDirFile.getName()).getVisibility() ==
          LocalResourceVisibility.PRIVATE);
    } finally {
      for(File f : new File[] { subDirFile, privateFile, publicFile, subDir, topLevelDir }) {
        if (f != null) {
          f.delete();
        }
      }
    }
  }

  private File createDirectoryOrFile(File parent, String child, boolean isDir) throws IOException {
    File file = new File(parent, child);
    if (!file.exists()) {
      if (isDir) {
        Assert.assertTrue(String.format("creating dir %s", file.getPath()), file.mkdirs());
        file.setWritable(true);
      } else {
        Assert.assertTrue(String.format("creating file %s", file.getPath()), file.createNewFile());
      }
    }

    return file;
  }

}
