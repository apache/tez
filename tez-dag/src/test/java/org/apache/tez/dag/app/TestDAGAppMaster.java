/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app;

import org.apache.hadoop.yarn.util.MonotonicClock;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.records.TezVertexID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.client.TezApiVersionInfo;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.AMPluginDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.api.records.DAGProtos.PlanLocalResourcesProto;
import org.apache.tez.dag.api.records.DAGProtos.TezNamedEntityDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.TezUserPayloadProto;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.app.rm.TaskSchedulerManager;
import org.apache.tez.dag.records.TezDAGID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDAGAppMaster {

  private static final String TEST_KEY = "TEST_KEY";
  private static final String TEST_VAL = "TEST_VAL";
  private static final String TS_NAME = "TS";
  private static final String CL_NAME = "CL";
  private static final String TC_NAME = "TC";
  private static final String CLASS_SUFFIX = "_CLASS";
  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")),
          TestDAGAppMaster.class.getSimpleName()).getAbsoluteFile();

  @Before
  public void setup() {
    FileUtil.fullyDelete(TEST_DIR);
    TEST_DIR.mkdir();
  }

  @After
  public void teardown() {
    FileUtil.fullyDelete(TEST_DIR);
  }

  @Test(timeout = 20000)
  public void testInvalidSession() throws Exception {
    // AM should fail if not the first attempt and in session mode and
    // DAG recovery is disabled, otherwise the app can succeed without
    // finishing an in-progress DAG.
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 2);
    DAGAppMasterForTest dam = new DAGAppMasterForTest(attemptId, true);
    TezConfiguration conf = new TezConfiguration(false);
    conf.setBoolean(TezConfiguration.DAG_RECOVERY_ENABLED, false);
    dam.init(conf);
    dam.start();
    verify(dam.mockScheduler).setShouldUnregisterFlag();
    verify(dam.mockShutdown).shutdown();
    List<String> diags = dam.getDiagnostics();
    boolean found = false;
    for (String diag : diags) {
      if (diag.contains(DAGAppMaster.INVALID_SESSION_ERR_MSG)) {
        found = true;
        break;
      }
    }
    Assert.assertTrue("Missing invalid session diagnostics", found);
    dam.stop();
  }

  @Test(timeout = 5000)
  public void testPluginParsing() throws IOException {
    BiMap<String, Integer> pluginMap = HashBiMap.create();
    Configuration conf = new Configuration(false);
    conf.set("testkey", "testval");
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);

    List<TezNamedEntityDescriptorProto> entityDescriptors = new LinkedList<>();
    List<NamedEntityDescriptor> entities;

    // Test empty descriptor list, yarn enabled
    pluginMap.clear();
    entities = new LinkedList<>();
    DAGAppMaster.parsePlugin(entities, pluginMap, null, true, false, defaultPayload);
    assertEquals(1, pluginMap.size());
    assertEquals(1, entities.size());
    assertTrue(pluginMap.containsKey(TezConstants.getTezYarnServicePluginName()));
    assertTrue(0 == pluginMap.get(TezConstants.getTezYarnServicePluginName()));
    assertEquals("testval",
        TezUtils.createConfFromUserPayload(entities.get(0).getUserPayload()).get("testkey"));

    // Test empty descriptor list, uber enabled
    pluginMap.clear();
    entities = new LinkedList<>();
    DAGAppMaster.parsePlugin(entities, pluginMap, null, false, true, defaultPayload);
    assertEquals(1, pluginMap.size());
    assertEquals(1, entities.size());
    assertTrue(pluginMap.containsKey(TezConstants.getTezUberServicePluginName()));
    assertTrue(0 == pluginMap.get(TezConstants.getTezUberServicePluginName()));
    assertEquals("testval",
        TezUtils.createConfFromUserPayload(entities.get(0).getUserPayload()).get("testkey"));

    // Test empty descriptor list, yarn enabled, uber enabled
    pluginMap.clear();
    entities = new LinkedList<>();
    DAGAppMaster.parsePlugin(entities, pluginMap, null, true, true, defaultPayload);
    assertEquals(2, pluginMap.size());
    assertEquals(2, entities.size());
    assertTrue(pluginMap.containsKey(TezConstants.getTezYarnServicePluginName()));
    assertTrue(0 == pluginMap.get(TezConstants.getTezYarnServicePluginName()));
    assertTrue(pluginMap.containsKey(TezConstants.getTezUberServicePluginName()));
    assertTrue(1 == pluginMap.get(TezConstants.getTezUberServicePluginName()));


    String pluginName = "d1";
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    TezNamedEntityDescriptorProto d1 =
        TezNamedEntityDescriptorProto.newBuilder().setName(pluginName).setEntityDescriptor(
            DAGProtos.TezEntityDescriptorProto.newBuilder().setClassName("d1Class")
                .setTezUserPayload(
                    TezUserPayloadProto.newBuilder()
                        .setUserPayload(ByteString.copyFrom(bb)))).build();
    entityDescriptors.add(d1);

    // Test descriptor, no yarn, no uber
    pluginMap.clear();
    entities = new LinkedList<>();
    DAGAppMaster.parsePlugin(entities, pluginMap, entityDescriptors, false, false, defaultPayload);
    assertEquals(1, pluginMap.size());
    assertEquals(1, entities.size());
    assertTrue(pluginMap.containsKey(pluginName));
    assertTrue(0 == pluginMap.get(pluginName));

    // Test descriptor, yarn and uber
    pluginMap.clear();
    entities = new LinkedList<>();
    DAGAppMaster.parsePlugin(entities, pluginMap, entityDescriptors, true, true, defaultPayload);
    assertEquals(3, pluginMap.size());
    assertEquals(3, entities.size());
    assertTrue(pluginMap.containsKey(TezConstants.getTezYarnServicePluginName()));
    assertTrue(0 == pluginMap.get(TezConstants.getTezYarnServicePluginName()));
    assertTrue(pluginMap.containsKey(TezConstants.getTezUberServicePluginName()));
    assertTrue(1 == pluginMap.get(TezConstants.getTezUberServicePluginName()));
    assertTrue(pluginMap.containsKey(pluginName));
    assertTrue(2 == pluginMap.get(pluginName));
    entityDescriptors.clear();
  }


  @Test(timeout = 5000)
  public void testParseAllPluginsNoneSpecified() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(TEST_KEY, TEST_VAL);
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);

    List<NamedEntityDescriptor> tsDescriptors;
    BiMap<String, Integer> tsMap;
    List<NamedEntityDescriptor> clDescriptors;
    BiMap<String, Integer> clMap;
    List<NamedEntityDescriptor> tcDescriptors;
    BiMap<String, Integer> tcMap;


    // No plugins. Non local
    tsDescriptors = Lists.newLinkedList();
    tsMap = HashBiMap.create();
    clDescriptors = Lists.newLinkedList();
    clMap = HashBiMap.create();
    tcDescriptors = Lists.newLinkedList();
    tcMap = HashBiMap.create();
    DAGAppMaster.parseAllPlugins(tsDescriptors, tsMap, clDescriptors, clMap, tcDescriptors, tcMap,
        null, false, defaultPayload);
    verifyDescAndMap(tsDescriptors, tsMap, 1, true, TezConstants.getTezYarnServicePluginName());
    verifyDescAndMap(clDescriptors, clMap, 1, true, TezConstants.getTezYarnServicePluginName());
    verifyDescAndMap(tcDescriptors, tcMap, 1, true, TezConstants.getTezYarnServicePluginName());

    // No plugins. Local
    tsDescriptors = Lists.newLinkedList();
    tsMap = HashBiMap.create();
    clDescriptors = Lists.newLinkedList();
    clMap = HashBiMap.create();
    tcDescriptors = Lists.newLinkedList();
    tcMap = HashBiMap.create();
    DAGAppMaster.parseAllPlugins(tsDescriptors, tsMap, clDescriptors, clMap, tcDescriptors, tcMap,
        null, true, defaultPayload);
    verifyDescAndMap(tsDescriptors, tsMap, 1, true, TezConstants.getTezUberServicePluginName());
    verifyDescAndMap(clDescriptors, clMap, 1, true, TezConstants.getTezUberServicePluginName());
    verifyDescAndMap(tcDescriptors, tcMap, 1, true, TezConstants.getTezUberServicePluginName());
  }

  @Test(timeout = 5000)
  public void testParseAllPluginsOnlyCustomSpecified() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(TEST_KEY, TEST_VAL);
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);
    TezUserPayloadProto payloadProto = TezUserPayloadProto.newBuilder()
        .setUserPayload(ByteString.copyFrom(defaultPayload.getPayload())).build();

    AMPluginDescriptorProto proto = createAmPluginDescriptor(false, false, true, payloadProto);

    List<NamedEntityDescriptor> tsDescriptors;
    BiMap<String, Integer> tsMap;
    List<NamedEntityDescriptor> clDescriptors;
    BiMap<String, Integer> clMap;
    List<NamedEntityDescriptor> tcDescriptors;
    BiMap<String, Integer> tcMap;


    // Only plugin, Yarn.
    tsDescriptors = Lists.newLinkedList();
    tsMap = HashBiMap.create();
    clDescriptors = Lists.newLinkedList();
    clMap = HashBiMap.create();
    tcDescriptors = Lists.newLinkedList();
    tcMap = HashBiMap.create();
    DAGAppMaster.parseAllPlugins(tsDescriptors, tsMap, clDescriptors, clMap, tcDescriptors, tcMap,
        proto, false, defaultPayload);
    verifyDescAndMap(tsDescriptors, tsMap, 2, true, TS_NAME,
        TezConstants.getTezYarnServicePluginName());
    verifyDescAndMap(clDescriptors, clMap, 1, true, CL_NAME);
    verifyDescAndMap(tcDescriptors, tcMap, 1, true, TC_NAME);
    assertEquals(TS_NAME + CLASS_SUFFIX, tsDescriptors.get(0).getClassName());
    assertEquals(CL_NAME + CLASS_SUFFIX, clDescriptors.get(0).getClassName());
    assertEquals(TC_NAME + CLASS_SUFFIX, tcDescriptors.get(0).getClassName());
  }

  @Test(timeout = 5000)
  public void testParseAllPluginsCustomAndYarnSpecified() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(TEST_KEY, TEST_VAL);
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);
    TezUserPayloadProto payloadProto = TezUserPayloadProto.newBuilder()
        .setUserPayload(ByteString.copyFrom(defaultPayload.getPayload())).build();

    AMPluginDescriptorProto proto = createAmPluginDescriptor(true, false, true, payloadProto);

    List<NamedEntityDescriptor> tsDescriptors;
    BiMap<String, Integer> tsMap;
    List<NamedEntityDescriptor> clDescriptors;
    BiMap<String, Integer> clMap;
    List<NamedEntityDescriptor> tcDescriptors;
    BiMap<String, Integer> tcMap;


    // Only plugin, Yarn.
    tsDescriptors = Lists.newLinkedList();
    tsMap = HashBiMap.create();
    clDescriptors = Lists.newLinkedList();
    clMap = HashBiMap.create();
    tcDescriptors = Lists.newLinkedList();
    tcMap = HashBiMap.create();
    DAGAppMaster.parseAllPlugins(tsDescriptors, tsMap, clDescriptors, clMap, tcDescriptors, tcMap,
        proto, false, defaultPayload);
    verifyDescAndMap(tsDescriptors, tsMap, 2, true, TezConstants.getTezYarnServicePluginName(),
        TS_NAME);
    verifyDescAndMap(clDescriptors, clMap, 2, true, TezConstants.getTezYarnServicePluginName(),
        CL_NAME);
    verifyDescAndMap(tcDescriptors, tcMap, 2, true, TezConstants.getTezYarnServicePluginName(),
        TC_NAME);
    assertNull(tsDescriptors.get(0).getClassName());
    assertNull(clDescriptors.get(0).getClassName());
    assertNull(tcDescriptors.get(0).getClassName());
    assertEquals(TS_NAME + CLASS_SUFFIX, tsDescriptors.get(1).getClassName());
    assertEquals(CL_NAME + CLASS_SUFFIX, clDescriptors.get(1).getClassName());
    assertEquals(TC_NAME + CLASS_SUFFIX, tcDescriptors.get(1).getClassName());
  }

  private void verifyDescAndMap(List<NamedEntityDescriptor> descriptors, BiMap<String, Integer> map,
                                int numExpected, boolean verifyPayload,
                                String... expectedNames) throws
      IOException {
    Preconditions.checkArgument(expectedNames.length == numExpected);
    assertEquals(numExpected, descriptors.size());
    assertEquals(numExpected, map.size());
    for (int i = 0; i < numExpected; i++) {
      assertEquals(expectedNames[i], descriptors.get(i).getEntityName());
      if (verifyPayload) {
        assertEquals(TEST_VAL,
            TezUtils.createConfFromUserPayload(descriptors.get(0).getUserPayload()).get(TEST_KEY));
      }
      assertTrue(map.get(expectedNames[i]) == i);
      assertTrue(map.inverse().get(i) == expectedNames[i]);
    }
  }

  private AMPluginDescriptorProto createAmPluginDescriptor(boolean enableYarn, boolean enableUber,
                                                           boolean addCustom,
                                                           TezUserPayloadProto payloadProto) {
    AMPluginDescriptorProto.Builder builder = AMPluginDescriptorProto.newBuilder()
        .setUberEnabled(enableUber)
        .setContainersEnabled(enableYarn);
    if (addCustom) {
      builder.addTaskSchedulers(
          TezNamedEntityDescriptorProto.newBuilder()
              .setName(TS_NAME)
              .setEntityDescriptor(
                  DAGProtos.TezEntityDescriptorProto.newBuilder()
                      .setClassName(TS_NAME + CLASS_SUFFIX)
                      .setTezUserPayload(payloadProto)))
          .addContainerLaunchers(
              TezNamedEntityDescriptorProto.newBuilder()
                  .setName(CL_NAME)
                  .setEntityDescriptor(
                      DAGProtos.TezEntityDescriptorProto.newBuilder()
                          .setClassName(CL_NAME + CLASS_SUFFIX)
                          .setTezUserPayload(payloadProto)))
          .addTaskCommunicators(
              TezNamedEntityDescriptorProto.newBuilder()
                  .setName(TC_NAME)
                  .setEntityDescriptor(
                      DAGProtos.TezEntityDescriptorProto.newBuilder()
                          .setClassName(TC_NAME + CLASS_SUFFIX)
                          .setTezUserPayload(payloadProto)));
    }
    return builder.build();
  }


  @Test
  public void testDagCredentialsWithoutMerge() throws Exception {
    testDagCredentials(false);
  }

  @Test
  public void testDagCredentialsWithMerge() throws Exception {
    testDagCredentials(true);
  }

  @Test
  public void testBadProgress() throws Exception {
    TezConfiguration conf = new TezConfiguration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CREDENTIALS_MERGE, true);
    conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, TEST_DIR.toString());
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);

    // create some sample AM credentials
    Credentials amCreds = new Credentials();
    JobTokenSecretManager jtsm = new JobTokenSecretManager();
    JobTokenIdentifier identifier = new JobTokenIdentifier(
        new Text(appId.toString()));
    Token<JobTokenIdentifier> sessionToken =
        new Token<JobTokenIdentifier>(identifier, jtsm);
    sessionToken.setService(identifier.getJobId());
    TokenCache.setSessionToken(sessionToken, amCreds);
    TestTokenSecretManager ttsm = new TestTokenSecretManager();
    Text tokenAlias1 = new Text("alias1");
    Token<TestTokenIdentifier> amToken1 = new Token<TestTokenIdentifier>(
        new TestTokenIdentifier(new Text("amtoken1")), ttsm);
    amCreds.addToken(tokenAlias1, amToken1);

    FileSystem fs = FileSystem.getLocal(conf);
    FSDataOutputStream sessionJarsPBOutStream =
        TezCommonUtils.createFileForAM(fs, new Path(TEST_DIR.toString(),
            TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
    DAGProtos.PlanLocalResourcesProto.getDefaultInstance()
        .writeDelimitedTo(sessionJarsPBOutStream);
    sessionJarsPBOutStream.close();
    DAGAppMaster am = spy(new DAGAppMaster(attemptId,
        ContainerId.newContainerId(attemptId, 1),
        "127.0.0.1", 0, 0, new MonotonicClock(), 1, true,
        TEST_DIR.toString(), new String[] {TEST_DIR.toString()},
        new String[] {TEST_DIR.toString()},
        new TezApiVersionInfo().getVersion(), amCreds,
        "someuser", null));
    when(am.getState()).thenReturn(DAGAppMasterState.RUNNING);
    am.init(conf);
    am.start();
    Credentials dagCreds = new Credentials();
    Token<TestTokenIdentifier> dagToken1 = new Token<TestTokenIdentifier>(
        new TestTokenIdentifier(new Text("dagtoken1")), ttsm);
    dagCreds.addToken(tokenAlias1, dagToken1);
    Text tokenAlias3 = new Text("alias3");
    Token<TestTokenIdentifier> dagToken2 = new Token<TestTokenIdentifier>(
        new TestTokenIdentifier(new Text("dagtoken2")), ttsm);
    dagCreds.addToken(tokenAlias3, dagToken2);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    DAGPlan dagPlan = DAGPlan.newBuilder()
        .setName("somedag")
        .setCredentialsBinary(
            DagTypeConverters.convertCredentialsToProto(dagCreds))
        .build();
    DAGImpl dag = spy(am.createDAG(dagPlan, dagId));
    am.setCurrentDAG(dag);
    when(dag.getState()).thenReturn(DAGState.RUNNING);
    Map<TezVertexID, Vertex> map = new HashMap<TezVertexID, Vertex>();
    TezVertexID mockVertexID = mock(TezVertexID.class);
    Vertex mockVertex = mock(Vertex.class);
    when(mockVertex.getProgress()).thenReturn(Float.NaN);
    map.put(mockVertexID, mockVertex);
    when(dag.getVertices()).thenReturn(map);
    when(dag.getTotalVertices()).thenReturn(1);
    Assert.assertEquals("Progress was NaN and should be reported as 0",
        0, am.getProgress(), 0);
    when(mockVertex.getProgress()).thenReturn(-10f);
    Assert.assertEquals("Progress was negative and should be reported as 0",
        0, am.getProgress(), 0);
    when(mockVertex.getProgress()).thenReturn(10f);
    Assert.assertEquals("Progress was greater than 1 and should be reported as 0",
        0, am.getProgress(), 0);
  }

  @SuppressWarnings("deprecation")
  private void testDagCredentials(boolean doMerge) throws IOException {
    TezConfiguration conf = new TezConfiguration();
    conf.setBoolean(TezConfiguration.TEZ_AM_CREDENTIALS_MERGE, doMerge);
    conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    conf.set(TezConfiguration.TEZ_AM_STAGING_DIR, TEST_DIR.toString());
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);

    // create some sample AM credentials
    Credentials amCreds = new Credentials();
    JobTokenSecretManager jtsm = new JobTokenSecretManager();
    JobTokenIdentifier identifier = new JobTokenIdentifier(
        new Text(appId.toString()));
    Token<JobTokenIdentifier> sessionToken =
        new Token<JobTokenIdentifier>(identifier, jtsm);
    sessionToken.setService(identifier.getJobId());
    TokenCache.setSessionToken(sessionToken, amCreds);
    TestTokenSecretManager ttsm = new TestTokenSecretManager();
    Text tokenAlias1 = new Text("alias1");
    Token<TestTokenIdentifier> amToken1 = new Token<TestTokenIdentifier>(
        new TestTokenIdentifier(new Text("amtoken1")), ttsm);
    amCreds.addToken(tokenAlias1, amToken1);
    Text tokenAlias2 = new Text("alias2");
    Token<TestTokenIdentifier> amToken2 = new Token<TestTokenIdentifier>(
        new TestTokenIdentifier(new Text("amtoken2")), ttsm);
    amCreds.addToken(tokenAlias2, amToken2);

    FileSystem fs = FileSystem.getLocal(conf);
    FSDataOutputStream sessionJarsPBOutStream =
        TezCommonUtils.createFileForAM(fs, new Path(TEST_DIR.toString(),
            TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
    DAGProtos.PlanLocalResourcesProto.getDefaultInstance()
        .writeDelimitedTo(sessionJarsPBOutStream);
    sessionJarsPBOutStream.close();
    DAGAppMaster am = new DAGAppMaster(attemptId,
        ContainerId.newInstance(attemptId, 1),
        "127.0.0.1", 0, 0, new SystemClock(), 1, true,
        TEST_DIR.toString(), new String[] {TEST_DIR.toString()},
        new String[] {TEST_DIR.toString()},
        new TezApiVersionInfo().getVersion(), amCreds,
        "someuser", null);
    am.init(conf);
    am.start();

    // create some sample DAG credentials
    Credentials dagCreds = new Credentials();
    Token<TestTokenIdentifier> dagToken1 = new Token<TestTokenIdentifier>(
        new TestTokenIdentifier(new Text("dagtoken1")), ttsm);
    dagCreds.addToken(tokenAlias2, dagToken1);
    Text tokenAlias3 = new Text("alias3");
    Token<TestTokenIdentifier> dagToken2 = new Token<TestTokenIdentifier>(
        new TestTokenIdentifier(new Text("dagtoken2")), ttsm);
    dagCreds.addToken(tokenAlias3, dagToken2);

    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    DAGPlan dagPlan = DAGPlan.newBuilder()
        .setName("somedag")
        .setCredentialsBinary(
            DagTypeConverters.convertCredentialsToProto(dagCreds))
        .build();
    DAGImpl dag = am.createDAG(dagPlan, dagId);
    Credentials fetchedDagCreds = dag.getCredentials();
    am.stop();

    Token<? extends TokenIdentifier> fetchedToken1 =
        fetchedDagCreds.getToken(tokenAlias1);
    if (doMerge) {
      assertNotNull("AM creds missing from DAG creds", fetchedToken1);
      compareTestTokens(amToken1, fetchedDagCreds.getToken(tokenAlias1));
    } else {
      assertNull("AM creds leaked to DAG creds", fetchedToken1);
    }
    compareTestTokens(dagToken1, fetchedDagCreds.getToken(tokenAlias2));
    compareTestTokens(dagToken2, fetchedDagCreds.getToken(tokenAlias3));
  }

  private static void compareTestTokens(
      Token<? extends TokenIdentifier> expected,
      Token<? extends TokenIdentifier> actual) throws IOException {
    TestTokenIdentifier expectedId = getTestTokenIdentifier(expected);
    TestTokenIdentifier actualId = getTestTokenIdentifier(actual);
    assertEquals("Token id not preserved", expectedId.getTestId(),
        actualId.getTestId());
  }

  private static TestTokenIdentifier getTestTokenIdentifier(
      Token<? extends TokenIdentifier> token) throws IOException {
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    TestTokenIdentifier tokenId = new TestTokenIdentifier();
    tokenId.readFields(in);
    in.close();
    return tokenId;
  }

  private static class TestTokenIdentifier extends TokenIdentifier {
    private static Text KIND_NAME = new Text("test-token");

    private Text testId;

    public TestTokenIdentifier() {
      this(new Text());
    }

    public TestTokenIdentifier(Text id) {
      testId = id;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      testId.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      testId.write(out);
    }

    @Override
    public Text getKind() {
      return KIND_NAME;
    }

    @Override
    public UserGroupInformation getUser() {
      return UserGroupInformation.createRemoteUser("token-user");
    }

    public Text getTestId() {
      return testId;
    }
  }

  private static class TestTokenSecretManager extends
      SecretManager<TestTokenIdentifier> {
    @Override
    public byte[] createPassword(TestTokenIdentifier id) {
      return id.getBytes();
    }

    @Override
    public byte[] retrievePassword(TestTokenIdentifier id)
        throws InvalidToken {
      return id.getBytes();
    }

    @Override
    public TestTokenIdentifier createIdentifier() {
      return new TestTokenIdentifier();
    }
  }

  private static class DAGAppMasterForTest extends DAGAppMaster {
    private DAGAppMasterShutdownHandler mockShutdown;
    private TaskSchedulerManager mockScheduler = mock(TaskSchedulerManager.class);

    public DAGAppMasterForTest(ApplicationAttemptId attemptId, boolean isSession) {
      super(attemptId, ContainerId.newContainerId(attemptId, 1), "hostname", 12345, 12346,
          new SystemClock(), 0, isSession, TEST_DIR.getAbsolutePath(),
          new String[] { TEST_DIR.getAbsolutePath() }, new String[] { TEST_DIR.getAbsolutePath() },
          new TezDagVersionInfo().getVersion(), createCredentials(), "jobname", null);
    }

    private static Credentials createCredentials() {
      Credentials creds = new Credentials();
      JobTokenSecretManager jtsm = new JobTokenSecretManager();
      JobTokenIdentifier jtid = new JobTokenIdentifier(new Text());
      Token<JobTokenIdentifier> token = new Token<JobTokenIdentifier>(jtid, jtsm);
      TokenCache.setSessionToken(token, creds);
      return creds;
    }

    private static void stubSessionResources() throws IOException {
      FileOutputStream out = new FileOutputStream(
          new File(TEST_DIR, TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME));
      PlanLocalResourcesProto planProto = PlanLocalResourcesProto.getDefaultInstance();
      planProto.writeDelimitedTo(out);
      out.close();
    }

    @Override
    public synchronized void serviceInit(Configuration conf) throws Exception {
      stubSessionResources();
      conf.setBoolean(TezConfiguration.TEZ_AM_WEBSERVICE_ENABLE, false);
      super.serviceInit(conf);
    }

    @Override
    protected DAGAppMasterShutdownHandler createShutdownHandler() {
      mockShutdown = mock(DAGAppMasterShutdownHandler.class);
      return mockShutdown;
    }

    @Override
    protected TaskSchedulerManager createTaskSchedulerManager(
        List<NamedEntityDescriptor> taskSchedulerDescriptors) {
      return mockScheduler;
    }
  }
}
