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

package org.apache.tez.common.security;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper.MockFileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestTokenCache {

  private static Configuration conf;

  private static String renewer;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new Configuration();
    conf.set(YarnConfiguration.RM_PRINCIPAL, "mapred/host@REALM");
    conf.setSocketAddr(YarnConfiguration.RM_ADDRESS,
        InetSocketAddress.createUnresolved("127.0.0.1", 8032));
    renewer = Master.getMasterPrincipal(conf);
  }

  @Test(timeout = 5000)
  @SuppressWarnings("deprecation")
  public void testBinaryCredentials() throws Exception {
    String binaryTokenFile = null;
    try {
      Path TEST_ROOT_DIR = new Path("target");
      binaryTokenFile = FileSystem.getLocal(conf).makeQualified(
        new Path(TEST_ROOT_DIR, "tokenFile")).toUri().getPath();

      MockFileSystem fs1 = createFileSystemForServiceName("service1");
      MockFileSystem fs2 = createFileSystemForServiceName("service2");

      // get the tokens for fs1 & fs2 and write out to binary creds file
      Credentials creds = new Credentials();
      Token<?> token1 = fs1.getDelegationToken(renewer);
      Token<?> token2 = fs2.getDelegationToken(renewer);
      creds.addToken(token1.getService(), token1);
      creds.addToken(token2.getService(), token2);
      creds.writeTokenStorageFile(new Path(binaryTokenFile), conf);


      Credentials newCreds = new Credentials();
      TokenCache.mergeBinaryTokens(newCreds, conf, binaryTokenFile);

      Assert.assertTrue(newCreds.getAllTokens().size() > 0);
      checkTokens(creds, newCreds);
    } finally {
      if (binaryTokenFile != null) {
        try {
          FileSystem.getLocal(conf).delete(new Path(binaryTokenFile));
        } catch (IOException e) {
          // Ignore
        }
      }
    }
  }

  @Test(timeout=5000)
  public void testObtainTokensForFileSystems() throws Exception {
    Path[] paths = makePaths(100, "test://dir/file");
    Credentials creds = new Credentials();
    Configuration conf = new Configuration(TestTokenCache.conf);
    conf.set("fs.test.impl", TestFileSystem.class.getName());

    // Cache enabled should be invoked only once
    conf.setBoolean("fs.test.impl.disable.cache", false);
    TokenCache.obtainTokensForFileSystemsInternal(creds, paths, conf);
    verify(TestFileSystem.fs, times(1)).addDelegationTokens(renewer, creds);

    // Cache disabled should be invoked for every path.
    conf.setBoolean("fs.test.impl.disable.cache", true);
    TokenCache.obtainTokensForFileSystemsInternal(creds, paths, conf);
    verify(TestFileSystem.fs, times(paths.length + 1)).addDelegationTokens(renewer, creds);

    // Excluded filesystem tokens should not be obtained.
    conf.set("tez.job.fs-servers.token-renewal.exclude", "dir");
    TokenCache.obtainTokensForFileSystemsInternal(creds, paths, conf);
    verify(TestFileSystem.fs, times(paths.length + 1)).addDelegationTokens(renewer, creds);
  }

  private Path[] makePaths(int count, String prefix) throws Exception {
    Path[] ps = new Path[count];
    for (int i = 0; i < count; ++i) {
      ps[i] = new Path(prefix + i);
    }
    return ps;
  }

  public static class TestFileSystem extends FilterFileSystem {
    static final FileSystem fs = mock(FileSystem.class);
    static {
      try {
        when(fs.getUri()).thenReturn(new URI("test://dir"));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    public TestFileSystem() {
      super(fs);
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials)
        throws IOException {
      return fs.addDelegationTokens(renewer, credentials);
    }
  }

  private MockFileSystem createFileSystemForServiceName(final String service)
      throws IOException {
    MockFileSystem mockFs = new MockFileSystem();
    when(mockFs.getCanonicalServiceName()).thenReturn(service);
    when(mockFs.getDelegationToken(any(String.class))).thenAnswer(
        new Answer<Token<?>>() {
          int unique = 0;
          @Override
          public Token<?> answer(InvocationOnMock invocation) throws Throwable {
            Token<?> token = new Token<TokenIdentifier>();
            token.setService(new Text(service));
            // use unique value so when we restore from token storage, we can
            // tell if it's really the same token
            token.setKind(new Text("token" + unique++));
            return token;
          }
        });
    return mockFs;
  }

  private void checkTokens(Credentials creds, Credentials newCreds) {
    Assert.assertEquals(creds.getAllTokens().size(),
        newCreds.getAllTokens().size());
    for (Token<?> token : newCreds.getAllTokens()) {
      Token<?> credsToken = creds.getToken(token.getService());
      Assert.assertTrue(credsToken != null);
      Assert.assertEquals(token, credsToken);
    }
  }

}
