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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.serviceplugins.api.ServicePluginsDescriptor;
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
      Map<String,LocalResource> resources = new HashMap<String, LocalResource>();
      TezClientUtils.setupTezJarsLocalResources(conf, credentials, resources);
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
      Map<String,LocalResource> resources = new HashMap<String, LocalResource>();
      TezClientUtils.setupTezJarsLocalResources(conf, credentials, resources);
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
    Map<String,LocalResource> resources = new HashMap<String, LocalResource>();
    TezClientUtils.setupTezJarsLocalResources(conf, credentials, resources);
  }

  /**
   *
   */
  @Test (timeout=10000)
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
    Map<String, LocalResource> localizedMap = new HashMap<String, LocalResource>();
    boolean usingArchive = TezClientUtils.setupTezJarsLocalResources(conf, credentials,
        localizedMap);
    Assert.assertFalse(usingArchive);
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
    Map<String, LocalResource> localizedMap = new HashMap<String, LocalResource>();
    Assert.assertFalse(TezClientUtils.setupTezJarsLocalResources(conf, credentials, localizedMap));
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
    Map<String, LocalResource> localizedMap = new HashMap<String, LocalResource>();
    Assert.assertFalse(TezClientUtils.setupTezJarsLocalResources(conf, credentials, localizedMap));
    assertFalse(localizedMap.isEmpty());
  }


    /**
   *
   */
  @Test (timeout=5000)
  public void testTezDefaultFS() throws Exception {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    StringBuilder tezLibUris = new StringBuilder();

    Path topDir = new Path(TEST_ROOT_DIR, "testTezDefaultFS");
    if (localFs.exists(topDir)) {
      localFs.delete(topDir, true);
    }
    localFs.mkdirs(topDir);

    Path file = new Path(topDir, "file.jar");

    Assert.assertTrue(localFs.createNewFile(file));
    tezLibUris.append(localFs.makeQualified(file).toString());

    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_LIB_URIS, tezLibUris.toString());
    conf.set("fs.defaultFS", "hdfs:///localhost:1234");
    Credentials credentials = new Credentials();
    Map<String, LocalResource> localizedMap = new HashMap<String, LocalResource>();
    TezClientUtils.setupTezJarsLocalResources(conf, credentials, localizedMap);

    Assert.assertTrue(localFs.delete(file, true));
    Assert.assertTrue(localFs.delete(topDir, true));
  }

    /**
   *
   */
  @Test (timeout=5000)
  public void validateSetTezJarLocalResourcesMultipleTarballs() throws Exception {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    StringBuilder tezLibUris = new StringBuilder();

    // Create 2 files
    Path topDir = new Path(TEST_ROOT_DIR, "validatemultipletarballs");
    if (localFs.exists(topDir)) {
      localFs.delete(topDir, true);
    }
    localFs.mkdirs(topDir);

    Path tarFile1 = new Path(topDir, "f1.tar.gz");
    Path tarFile2 = new Path(topDir, "f2.tar.gz");

    Assert.assertTrue(localFs.createNewFile(tarFile1));
    Assert.assertTrue(localFs.createNewFile(tarFile2));
    tezLibUris.append(localFs.makeQualified(tarFile1).toString()).append("#tar1").append(",");
    tezLibUris.append(localFs.makeQualified(tarFile2).toString()).append("#tar2").append(",");

    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_LIB_URIS, tezLibUris.toString());
    Credentials credentials = new Credentials();
    Map<String, LocalResource> localizedMap = new HashMap<String, LocalResource>();
    TezClientUtils.setupTezJarsLocalResources(conf, credentials, localizedMap);
    Set<String> resourceNames = localizedMap.keySet();
    Assert.assertEquals(2, resourceNames.size());
    Assert.assertTrue(resourceNames.contains("tar1"));
    Assert.assertTrue(resourceNames.contains("tar2"));
    Assert.assertFalse(resourceNames.contains("f1.tar.gz"));
    Assert.assertFalse(resourceNames.contains("f2.tar.gz"));


    Assert.assertTrue(localFs.delete(tarFile1, true));
    Assert.assertTrue(localFs.delete(tarFile2, true));
    Assert.assertTrue(localFs.delete(topDir, true));
  }

    /**
   *
   */
  @Test (timeout=5000)
  public void validateSetTezJarLocalResourcesMixTarballAndJar() throws Exception {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    StringBuilder tezLibUris = new StringBuilder();

    // Create 2 jars and 1 archive
    Path topDir = new Path(TEST_ROOT_DIR, "validatetarballandjar");
    if (localFs.exists(topDir)) {
      localFs.delete(topDir, true);
    }
    localFs.mkdirs(topDir);

    Path tarFile1 = new Path(topDir, "f1.tar.gz");
    Path jarFile2 = new Path(topDir, "f2.jar");
    Path jarFile3 = new Path(topDir, "f3.jar");

    Assert.assertTrue(localFs.createNewFile(tarFile1));
    Assert.assertTrue(localFs.createNewFile(jarFile2));
    Assert.assertTrue(localFs.createNewFile(jarFile3));

    tezLibUris.append(localFs.makeQualified(topDir).toString()).append(",");
    tezLibUris.append(localFs.makeQualified(tarFile1).toString()).append("#tar1").append(",");

    TezConfiguration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_LIB_URIS, tezLibUris.toString());
    Credentials credentials = new Credentials();
    Map<String, LocalResource> localizedMap = new HashMap<String, LocalResource>();
    TezClientUtils.setupTezJarsLocalResources(conf, credentials, localizedMap);
    Set<String> resourceNames = localizedMap.keySet();
    Assert.assertEquals(4, resourceNames.size());
    Assert.assertTrue(resourceNames.contains("tar1"));
    Assert.assertTrue(resourceNames.contains("f1.tar.gz"));
    Assert.assertTrue(resourceNames.contains("f2.jar"));
    Assert.assertTrue(resourceNames.contains("f3.jar"));

    Assert.assertTrue(localFs.delete(tarFile1, true));
    Assert.assertTrue(localFs.delete(jarFile2, true));
    Assert.assertTrue(localFs.delete(jarFile3, true));
    Assert.assertTrue(localFs.delete(topDir, true));
  }

  @Test(timeout = 2000)
  // this test checks if the priority field is set properly in the
  // ApplicationSubmissionContext
  public void testAppSubmissionContextForPriority() throws Exception {
    TezConfiguration tezConf = new TezConfiguration();
    int testpriority = 999;
    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    Credentials credentials = new Credentials();
    TezClientUtils.createSessionToken(appId.toString(),
        new JobTokenSecretManager(), credentials);
    tezConf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    Map<String, LocalResource> m = new HashMap<String, LocalResource>();
    tezConf.setInt(TezConfiguration.TEZ_AM_APPLICATION_PRIORITY, testpriority);
    AMConfiguration amConf =
        new AMConfiguration(tezConf, new HashMap<String, LocalResource>(), credentials);
    ApplicationSubmissionContext appcontext;
    appcontext = TezClientUtils.createApplicationSubmissionContext(
        appId, null, "dagname",
        amConf, m,
        credentials, false,
        new TezApiVersionInfo(), null, null);
    assertEquals(testpriority, appcontext.getPriority().getPriority());
  }

  @Test(timeout=1000)
  // when tez config property for app priority not set,
  // tez should not set app priority and let YARN deal with it.
  public void testSetApplicationPriority() {
    TezConfiguration conf = new TezConfiguration(false);
    AMConfiguration amconfig = new AMConfiguration(conf, null, null);
    ApplicationSubmissionContext appContext = Records
        .newRecord(ApplicationSubmissionContext.class);
    TezClientUtils.setApplicationPriority(appContext, amconfig);
    assertNull(appContext.getPriority());
  }

  @Test(timeout=1000)
  public void testSetApplicationTags() {
    TezConfiguration conf = new TezConfiguration(false);
    conf.set(TezConfiguration.TEZ_APPLICATION_TAGS, "foo,bar");
    AMConfiguration amconfig = new AMConfiguration(conf, null, null);
    ApplicationSubmissionContext appContext = Records
        .newRecord(ApplicationSubmissionContext.class);
    Collection<String> tagsFromConf =
        amconfig.getTezConfiguration().getTrimmedStringCollection(
        TezConfiguration.TEZ_APPLICATION_TAGS);
    appContext.setApplicationTags(new HashSet<String>(tagsFromConf));
    assertTrue(appContext.getApplicationTags().contains("foo"));
    assertTrue(appContext.getApplicationTags().contains("bar"));
  }

  @Test(timeout = 5000)
  public void testSessionTokenInAmClc() throws IOException, YarnException {

    TezConfiguration tezConf = new TezConfiguration();

    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    DAG dag = DAG.create("testdag");
    dag.addVertex(Vertex.create("testVertex", ProcessorDescriptor.create("processorClassname"), 1)
        .setTaskLaunchCmdOpts("initialLaunchOpts"));

    Credentials credentials = new Credentials();
    JobTokenSecretManager jobTokenSecretManager = new JobTokenSecretManager();
    TezClientUtils.createSessionToken(appId.toString(), jobTokenSecretManager, credentials);
    Token<JobTokenIdentifier> jobToken = TokenCache.getSessionToken(credentials);
    assertNotNull(jobToken);

    AMConfiguration amConf =
        new AMConfiguration(tezConf, new HashMap<String, LocalResource>(), credentials);
    ApplicationSubmissionContext appSubmissionContext =
        TezClientUtils.createApplicationSubmissionContext(appId, dag, "amName", amConf,
            new HashMap<String, LocalResource>(), credentials, false, new TezApiVersionInfo(),
            null, null);

    ContainerLaunchContext amClc = appSubmissionContext.getAMContainerSpec();
    Map<String, ByteBuffer> amServiceData = amClc.getServiceData();
    assertNotNull(amServiceData);
    assertEquals(1, amServiceData.size());

    DataInputByteBuffer dibb = new DataInputByteBuffer();
    dibb.reset(amServiceData.values().iterator().next());
    Token<JobTokenIdentifier> jtSent = new Token<JobTokenIdentifier>();
    jtSent.readFields(dibb);

    assertTrue(Arrays.equals(jobToken.getIdentifier(), jtSent.getIdentifier()));
  }

  @Test(timeout = 5000)
  public void testAMLoggingOptsSimple() throws IOException, YarnException {

    TezConfiguration tezConf = new TezConfiguration();
    tezConf.set(TezConfiguration.TEZ_AM_LOG_LEVEL, "WARN");

    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    Credentials credentials = new Credentials();
    JobTokenSecretManager jobTokenSecretManager = new JobTokenSecretManager();
    TezClientUtils.createSessionToken(appId.toString(), jobTokenSecretManager, credentials);
    DAG dag = DAG.create("testdag");
    dag.addVertex(Vertex.create("testVertex", ProcessorDescriptor.create("processorClassname"), 1)
        .setTaskLaunchCmdOpts("initialLaunchOpts"));
    AMConfiguration amConf =
        new AMConfiguration(tezConf, new HashMap<String, LocalResource>(), credentials);
    ApplicationSubmissionContext appSubmissionContext =
        TezClientUtils.createApplicationSubmissionContext(appId, dag, "amName", amConf,
            new HashMap<String, LocalResource>(), credentials, false, new TezApiVersionInfo(),
            null, null);

    List<String> expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add("-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "WARN" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    List<String> commands = appSubmissionContext.getAMContainerSpec().getCommands();
    assertEquals(1, commands.size());
    for (String expectedCmd : expectedCommands) {
      assertTrue(commands.get(0).contains(expectedCmd));
    }

    Map<String, String> environment = appSubmissionContext.getAMContainerSpec().getEnvironment();
    String logEnv = environment.get(TezConstants.TEZ_CONTAINER_LOG_PARAMS);
    assertNull(logEnv);
  }

  @Test(timeout = 5000)
  public void testAMLoggingOptsPerLogger() throws IOException, YarnException {

    TezConfiguration tezConf = new TezConfiguration();
    tezConf.set(TezConfiguration.TEZ_AM_LOG_LEVEL,
        "WARN;org.apache.hadoop.ipc=DEBUG;org.apache.hadoop.security=DEBUG");

    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    Credentials credentials = new Credentials();
    JobTokenSecretManager jobTokenSecretManager = new JobTokenSecretManager();
    TezClientUtils.createSessionToken(appId.toString(), jobTokenSecretManager, credentials);
    DAG dag = DAG.create("testdag");
    dag.addVertex(Vertex.create("testVertex", ProcessorDescriptor.create("processorClassname"), 1)
        .setTaskLaunchCmdOpts("initialLaunchOpts"));
    AMConfiguration amConf =
        new AMConfiguration(tezConf, new HashMap<String, LocalResource>(), credentials);
    ApplicationSubmissionContext appSubmissionContext =
        TezClientUtils.createApplicationSubmissionContext(appId, dag, "amName", amConf,
            new HashMap<String, LocalResource>(), credentials, false, new TezApiVersionInfo(),
            null, null);

    List<String> expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add(
        "-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "WARN" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    List<String> commands = appSubmissionContext.getAMContainerSpec().getCommands();
    assertEquals(1, commands.size());
    for (String expectedCmd : expectedCommands) {
      assertTrue(commands.get(0).contains(expectedCmd));
    }

    Map<String, String> environment = appSubmissionContext.getAMContainerSpec().getEnvironment();
    String logEnv = environment.get(TezConstants.TEZ_CONTAINER_LOG_PARAMS);
    assertEquals("org.apache.hadoop.ipc=DEBUG;org.apache.hadoop.security=DEBUG", logEnv);
  }

  @Test(timeout = 5000)
  public void testAMCommandOpts() {
    Path tmpDir = new Path(Environment.PWD.$(),
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    String tmpOpts = "-Djava.io.tmpdir=" + tmpDir;
    TezConfiguration tezConf = new TezConfiguration();
    String amCommandOpts = "-Xmx 200m -Dtest.property";
    tezConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, amCommandOpts);

    // Test1: Rely on defaults for cluster-default opts
    String amOptsConstructed =
        TezClientUtils.constructAMLaunchOpts(tezConf, Resource.newInstance(1024, 1));
    assertEquals(tmpOpts + " "
        + TezConfiguration.TEZ_AM_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS_DEFAULT + " "
        + amCommandOpts,
        amOptsConstructed);

    // Test2: Setup cluster-default command opts explicitly
    String clusterDefaultCommandOpts =
        "-server -Djava.net.preferIPv4Stack=true -XX:+PrintGCDetails -verbose:gc ";
    tezConf.set(TezConfiguration.TEZ_AM_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS, clusterDefaultCommandOpts);
    amOptsConstructed =
        TezClientUtils.constructAMLaunchOpts(tezConf, Resource.newInstance(1024, 1));
    assertEquals(tmpOpts + " " + clusterDefaultCommandOpts + " " + amCommandOpts, amOptsConstructed);


    // Test3: Don't setup Xmx explicitly
    final double factor = 0.8;
    amCommandOpts = "-Dtest.property";
    tezConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, amCommandOpts);
    amOptsConstructed =
        TezClientUtils.constructAMLaunchOpts(tezConf, Resource.newInstance(1024, 1));
    // It's OK for the Xmx value to show up before cluster default options, since Xmx will not be replaced if it already exists.
    assertEquals(
        " -Xmx" + ((int) (1024 * factor)) + "m" + " " + tmpOpts + " " + clusterDefaultCommandOpts + " " +
            amCommandOpts,
        amOptsConstructed);

    // Test4: Ensure admin options with Xmx does not cause them to be overridden. This should almost never be done though.
    clusterDefaultCommandOpts =
        "-server -Djava.net.preferIPv4Stack=true -XX:+PrintGCDetails -verbose:gc -Xmx200m";
    tezConf.set(TezConfiguration.TEZ_AM_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS, clusterDefaultCommandOpts);
    amOptsConstructed =
        TezClientUtils.constructAMLaunchOpts(tezConf, Resource.newInstance(1024, 1));
    assertEquals(tmpOpts + " " + clusterDefaultCommandOpts + " " + amCommandOpts, amOptsConstructed);
  }

  @Test(timeout = 5000)
  public void testTaskCommandOpts() throws TezException {
    TezConfiguration tezConf = new TezConfiguration();
    String taskCommandOpts = "-Xmx 200m -Dtest.property";
    tezConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, taskCommandOpts);
    String expected = null;

    // Test1: Rely on defaults for cluster default opts
    String taskOptsConstructed = TezClientUtils.addDefaultsToTaskLaunchCmdOpts("", tezConf);
    expected =
        TezConfiguration.TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS_DEFAULT + " " + taskCommandOpts;
    assertTrue(
        "Did not find Expected prefix: [" + expected + "] in string [" + taskOptsConstructed +
            "]", taskOptsConstructed.startsWith(expected));

    // Test2: Setup cluster-default command opts explicitly
    String taskClusterDefaultCommandOpts =
        "-server -Djava.net.preferIPv4Stack=true -XX:+PrintGCDetails -verbose:gc ";
    tezConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS,
        taskClusterDefaultCommandOpts);
    taskOptsConstructed =
        TezClientUtils.addDefaultsToTaskLaunchCmdOpts("", tezConf);
    expected = taskClusterDefaultCommandOpts + " " + taskCommandOpts;
    assertTrue(
        "Did not find Expected prefix: [" + expected + "] in string [" + taskOptsConstructed +
            "]", taskOptsConstructed.startsWith(expected));

    // Test3: Don't setup Xmx explicitly
    taskCommandOpts = "-Dtest.property";
    tezConf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, taskCommandOpts);
    taskOptsConstructed =
        TezClientUtils.addDefaultsToTaskLaunchCmdOpts("", tezConf);
    expected = taskClusterDefaultCommandOpts + " " + taskCommandOpts;
    assertTrue(
        "Did not find Expected prefix: [" + expected + "] in string [" + taskOptsConstructed +
            "]", taskOptsConstructed.startsWith(expected));

    // Test4: Pass in a dag-configured value.
    String programmaticTaskOpts = "-Dset.programatically=true -Djava.net.preferIPv4Stack=false";
    taskOptsConstructed =
        TezClientUtils.addDefaultsToTaskLaunchCmdOpts(programmaticTaskOpts, tezConf);
    // Container logging is always added at the end, if it's required.
    expected = taskClusterDefaultCommandOpts + " " + taskCommandOpts + " " + programmaticTaskOpts;
    assertTrue(
        "Did not find Expected prefix: [" + expected + "] in string [" + taskOptsConstructed +
            "]", taskOptsConstructed.startsWith(expected));
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

    origJavaOpts = "";
    javaOpts = TezClientUtils.maybeAddDefaultMemoryJavaOpts(origJavaOpts,
        Resource.newInstance(1000, 1), -1);
    Assert.assertTrue(javaOpts.contains("-Xmx700m"));

    origJavaOpts = "";
    javaOpts = TezClientUtils.maybeAddDefaultMemoryJavaOpts(origJavaOpts,
        Resource.newInstance(5000, 1), -1);
    Assert.assertTrue(javaOpts.contains("-Xmx4000m"));
  }

  @Test (timeout=5000)
  public void testDefaultLoggingJavaOpts() {
    String origJavaOpts = null;
    String javaOpts = TezClientUtils.maybeAddDefaultLoggingJavaOpts("FOOBAR", origJavaOpts);
    Assert.assertNotNull(javaOpts);
    Assert.assertTrue(javaOpts.contains("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=FOOBAR")
        && javaOpts.contains(TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE)
        &&
        javaOpts.contains("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator"));
  }

  @Test (timeout = 5000)
  public void testConfSerializationForAm() {
    Configuration conf =new Configuration(false);
    String val1 = "fixedProperty";
    String val2 = "parametrizedProperty/${user.name}";
    String expVal2 = "parametrizedProperty/" + System.getProperty("user.name");
    conf.set("property1", val1);
    conf.set("property2", val2);

    Map<String, String> expected = new HashMap<String, String>();
    expected.put("property1", val1);
    expected.put("property2", expVal2);

    ConfigurationProto confProto = TezClientUtils.createFinalConfProtoForApp(conf, null);

    for (PlanKeyValuePair kvPair : confProto.getConfKeyValuesList()) {
      String v = expected.remove(kvPair.getKey());
      assertEquals(v, kvPair.getValue());
    }
    assertTrue(expected.isEmpty());
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

      Map<String, LocalResource> lrMap = new HashMap<String, LocalResource>();
      TezClientUtils.setupTezJarsLocalResources(tezConf, new Credentials(), lrMap);

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
      lrMap.clear();
      Assert.assertTrue(
          TezClientUtils.setupTezJarsLocalResources(tezConf, new Credentials(), lrMap));

      Assert.assertEquals(LocalResourceVisibility.PUBLIC,
          lrMap.get(TezConstants.TEZ_TAR_LR_NAME).getVisibility());

      //private
      remoteFs.setPermission(tarFile, privateFilePerms);
      lrMap.clear();
      TezClientUtils.setupTezJarsLocalResources(tezConf, new Credentials(), lrMap);
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
    confMap.put("foo.property", "2000");
    confMap.put("tez.property", "tezProperty");
    confMap.put("yarn.property", "yarnProperty");

    for (Map.Entry<String, String> entry : confMap.entrySet()) {
      srcConf.set(entry.getKey(), entry.getValue());
    }

    ConfigurationProto confProto = TezClientUtils.createFinalConfProtoForApp(srcConf, null);

    for (PlanKeyValuePair kvPair : confProto.getConfKeyValuesList()) {
      String val = confMap.remove(kvPair.getKey());
      assertNotNull(val);
      assertEquals(val, kvPair.getValue());
    }
    assertTrue(confMap.isEmpty());
  }

  private Path createFile(FileSystem fs, Path dir, String fileName) throws IOException {
    Path f1 = new Path(dir, fileName);
    FSDataOutputStream outputStream = fs.create(f1, true);
    outputStream.write(1);
    outputStream.close();
    return fs.makeQualified(f1);
  }

  @Test (timeout=5000)
  public void validateSetTezAuxLocalResourcesWithFilesAndFolders() throws Exception {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    List<String> resources = new ArrayList<String>();
    StringBuilder auxUriStr = new StringBuilder();

    // Create 2 files
    Path topDir = new Path(TEST_ROOT_DIR, "validateauxwithfiles");
    if (localFs.exists(topDir)) {
      localFs.delete(topDir, true);
    }
    localFs.mkdirs(topDir);
    resources.add(createFile(localFs, topDir, "f1.txt").toString());
    auxUriStr.append(localFs.makeQualified(topDir).toString()).append(",");

    Path dir2 = new Path(topDir, "dir2");
    localFs.mkdirs(dir2);
    Path nestedDir = new Path(dir2, "nestedDir");
    localFs.mkdirs(nestedDir);
    createFile(localFs, nestedDir, "nested-f.txt");
    resources.add(createFile(localFs, dir2, "dir2-f.txt").toString());
    auxUriStr.append(localFs.makeQualified(dir2).toString()).append(",");

    Path dir3 = new Path(topDir, "dir3");
    localFs.mkdirs(dir3);
    auxUriStr.append(localFs.makeQualified(dir3).toString()).append(",");

    TezConfiguration conf = new TezConfiguration();
    conf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    conf.set(TezConfiguration.TEZ_AUX_URIS, auxUriStr.toString());
    Credentials credentials = new Credentials();
    Map<String, LocalResource> localizedMap = new HashMap<String, LocalResource>();
    TezClientUtils.setupTezJarsLocalResources(conf, credentials, localizedMap);
    Set<String> resourceNames = localizedMap.keySet();
    Assert.assertEquals(2, resourceNames.size());
    Assert.assertTrue(resourceNames.contains("f1.txt"));
    Assert.assertTrue(resourceNames.contains("dir2-f.txt"));
  }

  @Test(timeout = 5000)
  public void testServiceDescriptorSerializationForAM() {
    Configuration conf = new Configuration(false);
    ServicePluginsDescriptor servicePluginsDescriptor = ServicePluginsDescriptor.create(true);

    ConfigurationProto confProto = TezClientUtils.createFinalConfProtoForApp(conf,
        servicePluginsDescriptor);

    assertTrue(confProto.hasAmPluginDescriptor());
    assertTrue(confProto.getAmPluginDescriptor().getUberEnabled());
  }

  @Test(timeout = 5000)
  public void testTaskLaunchCmdOptsSetup() throws TezException {
    Configuration conf = new Configuration(false);
    String vOpts = "";
    String opts = TezClientUtils.addDefaultsToTaskLaunchCmdOpts(vOpts, conf);

    Assert.assertEquals(opts,
        TezConfiguration.TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS_DEFAULT + " "
            + TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS_DEFAULT + " " + vOpts);

    vOpts = "foo";
    opts = TezClientUtils.addDefaultsToTaskLaunchCmdOpts(vOpts, conf);

    Assert.assertEquals(opts,
        TezConfiguration.TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS_DEFAULT + "  " + vOpts);

    String taskOpts = "taskOpts";
    conf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, taskOpts);
    opts = TezClientUtils.addDefaultsToTaskLaunchCmdOpts(vOpts, conf);

    Assert.assertEquals(opts,
        TezConfiguration.TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS_DEFAULT
            + " " + taskOpts + " " + vOpts);

  }

  @Test(timeout = 5000)
  public void testClusterTaskLaunchCmdOptsSetup() throws TezException {
    Configuration conf = new Configuration(false);
    String adminOpts = "adminOpts";
    conf.set(TezConfiguration.TEZ_TASK_LAUNCH_CLUSTER_DEFAULT_CMD_OPTS, adminOpts);

    String vOpts = "";
    String opts = TezClientUtils.addDefaultsToTaskLaunchCmdOpts(vOpts, conf);

    Assert.assertEquals(opts,
        adminOpts + " "
            + TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS_DEFAULT + " " + vOpts);

    vOpts = "foo";
    opts = TezClientUtils.addDefaultsToTaskLaunchCmdOpts(vOpts, conf);

    Assert.assertEquals(opts, adminOpts + "  " + vOpts);

    String taskOpts = "taskOpts";
    conf.set(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS, taskOpts);
    opts = TezClientUtils.addDefaultsToTaskLaunchCmdOpts(vOpts, conf);

    Assert.assertEquals(opts, adminOpts + " " + taskOpts + " " + vOpts);

  }


}
