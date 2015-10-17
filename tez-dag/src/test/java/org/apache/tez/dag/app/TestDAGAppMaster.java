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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
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
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.DAGPlan;
import org.apache.tez.dag.app.dag.impl.DAGImpl;
import org.apache.tez.dag.records.TezDAGID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDAGAppMaster {

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

  @Test
  public void testDagCredentialsWithoutMerge() throws Exception {
    testDagCredentials(false);
  }

  @Test
  public void testDagCredentialsWithMerge() throws Exception {
    testDagCredentials(true);
  }

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
        ContainerId.newContainerId(attemptId, 1),
        "127.0.0.1", 0, 0, new SystemClock(), 1, true,
        TEST_DIR.toString(), new String[] {TEST_DIR.toString()},
        new String[] {TEST_DIR.toString()},
        new TezApiVersionInfo().getVersion(), 1, amCreds,
        "someuser");
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
}
