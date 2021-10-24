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

import static org.apache.hadoop.security.ssl.SSLFactory.SSL_CLIENT_CONF_KEY;
import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.KeyPair;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.x500.X500Principal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileOutputStream;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.mapreduce.examples.TestOrderedWordCount;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.X509Extensions;
import org.bouncycastle.x509.X509V3CertificateGenerator;
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
  private boolean asyncHttp;

  public TestSecureShuffle(boolean sslInCluster, int resultWithTezSSL, int resultWithoutTezSSL,
      boolean asyncHttp) {
    this.enableSSLInCluster = sslInCluster;
    this.resultWithTezSSL = resultWithTezSSL;
    this.resultWithoutTezSSL = resultWithoutTezSSL;
    this.asyncHttp = asyncHttp;
  }

  @Parameterized.Parameters(name = "test[sslInCluster:{0}, resultWithTezSSL:{1}, "
      + "resultWithoutTezSSL:{2}, asyncHttp:{3}]")
  public static Collection<Object[]> getParameters() {
    Collection<Object[]> parameters = new ArrayList<Object[]>();
    //enable ssl in cluster, succeed with tez-ssl enabled, fail with tez-ssl disabled
    parameters.add(new Object[] { true, 0, 1, false });

    //With asyncHttp
    parameters.add(new Object[] { true, 0, 1, true });
    parameters.add(new Object[] { false, 1, 0, true });

    //Negative testcase
    //disable ssl in cluster, fail with tez-ssl enabled, succeed with tez-ssl disabled
    parameters.add(new Object[] { false, 1, 0, false });

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

    conf.setLong(TezConfiguration.TEZ_AM_SLEEP_TIME_BEFORE_EXIT_MILLIS, 500);
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_USE_ASYNC_HTTP, asyncHttp);
    String sslConf = conf.get(SSL_CLIENT_CONF_KEY, "ssl-client.xml");
    conf.addResource(sslConf);

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
  @Test(timeout = 500000)
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
    String sslConfsDir = KeyStoreTestUtil.getClasspathDir(TestSecureShuffle.class);

    setupSSLConfig(keysStoresDir.getAbsolutePath(), sslConfsDir, conf, true, true, "");
  }

  /**
   * This is a copied version of hadoop's KeyStoreTestUtil.setupSSLConfig which was needed to create
   * server certs with actual hostname in CN instead of "localhost". While upgrading async http
   * client in TEZ-4237, it turned out that netty doesn't support custom hostname verifiers anymore
   * (as discussed in https://github.com/AsyncHttpClient/async-http-client/issues/928), that's why
   * it cannot be set for an async http connection. So instead of hacking an ALLOW_ALL verifier
   * somehow (which cannot be propagated to netty), a valid certificate with the actual hostname
   * should be generated in setupSSLConfig, so the change is the usage of
   * "InetAddress.getLocalHost().getHostName()".
   */
  public static void setupSSLConfig(String keystoresDir, String sslConfDir, Configuration config,
      boolean useClientCert, boolean trustStore, String excludeCiphers) throws Exception {
    String clientKS = keystoresDir + "/clientKS.jks";
    String clientPassword = "clientP";
    String serverKS = keystoresDir + "/serverKS.jks";
    String serverPassword = "serverP";
    String trustKS = null;
    String trustPassword = "trustP";

    File sslClientConfFile = new File(sslConfDir, KeyStoreTestUtil.getClientSSLConfigFileName());
    File sslServerConfFile = new File(sslConfDir, KeyStoreTestUtil.getServerSSLConfigFileName());

    Map<String, X509Certificate> certs = new HashMap<String, X509Certificate>();

    if (useClientCert) {
      KeyPair cKP = KeyStoreTestUtil.generateKeyPair("RSA");
      X509Certificate cCert =
          generateCertificate("CN=localhost, O=client", cKP, 30, "SHA1withRSA");
      KeyStoreTestUtil.createKeyStore(clientKS, clientPassword, "client", cKP.getPrivate(), cCert);
      certs.put("client", cCert);
    }

    String localhostName = InetAddress.getLocalHost().getHostName();
    KeyPair sKP = KeyStoreTestUtil.generateKeyPair("RSA");
    X509Certificate sCert =
        generateCertificate("CN="+localhostName+", O=server", sKP, 30, "SHA1withRSA");
    KeyStoreTestUtil.createKeyStore(serverKS, serverPassword, "server", sKP.getPrivate(), sCert);
    certs.put("server", sCert);

    if (trustStore) {
      trustKS = keystoresDir + "/trustKS.jks";
      KeyStoreTestUtil.createTrustStore(trustKS, trustPassword, certs);
    }

    Configuration clientSSLConf = KeyStoreTestUtil.createClientSSLConfig(clientKS, clientPassword,
        clientPassword, trustKS, excludeCiphers);
    Configuration serverSSLConf = KeyStoreTestUtil.createServerSSLConfig(serverKS, serverPassword,
        serverPassword, trustKS, excludeCiphers);

    KeyStoreTestUtil.saveConfig(sslClientConfFile, clientSSLConf);
    KeyStoreTestUtil.saveConfig(sslServerConfFile, serverSSLConf);

    // this will be ignored for AsyncHttpConnection, see method comments above
    config.set(SSLFactory.SSL_HOSTNAME_VERIFIER_KEY, "ALLOW_ALL");

    config.set(SSLFactory.SSL_CLIENT_CONF_KEY, sslClientConfFile.getName());
    config.set(SSLFactory.SSL_SERVER_CONF_KEY, sslServerConfFile.getName());
    config.setBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY, useClientCert);
  }

  public static X509Certificate generateCertificate(String dn, KeyPair pair, int days, String algorithm)
      throws Exception {

    Date from = new Date();
    Date to = new Date(from.getTime() + days * 86400000l);
    BigInteger sn = new BigInteger(64, new SecureRandom());
    KeyPair keyPair = pair;
    X509V3CertificateGenerator certGen = new X509V3CertificateGenerator();

    String hostAddress = InetAddress.getLocalHost().getHostAddress();
    certGen.addExtension(X509Extensions.SubjectAlternativeName, false,
        new GeneralNames(new GeneralName(GeneralName.iPAddress, hostAddress)));

    X500Principal dnName = new X500Principal(dn);

    certGen.setSerialNumber(sn);
    certGen.setIssuerDN(dnName);
    certGen.setNotBefore(from);
    certGen.setNotAfter(to);
    certGen.setSubjectDN(dnName);
    certGen.setPublicKey(keyPair.getPublic());
    certGen.setSignatureAlgorithm(algorithm);

    X509Certificate cert = certGen.generate(pair.getPrivate());
    return cert;
  }
}
