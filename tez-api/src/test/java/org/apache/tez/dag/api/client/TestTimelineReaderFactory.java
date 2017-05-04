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

package org.apache.tez.dag.api.client;

import static org.mockito.Mockito.mock;

import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.TimelineReaderFactory.TimelineReaderPseudoAuthenticatedStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestTimelineReaderFactory {

  // ensure on hadoop 2.6+ TimelineReaderTokenAuthenticatedStrategy is used.
  @Test(timeout = 5000)
  public void testShouldUseTokenDelegationAuthStrategyForHadoop26() throws TezException {

    String returnedClassName =
        TimelineReaderFactory.getTimelineReaderStrategy(mock(Configuration.class), false, 0)
            .getClass()
            .getCanonicalName();
    Assert.assertEquals("should use pseudo auth on hadoop2.4",
        "org.apache.tez.dag.api.client.TimelineReaderFactory.TimelineReaderTokenAuthenticatedStrategy",
        returnedClassName);
  }

  @Test(timeout = 5000)
  public void testPseudoAuthenticatorConnectionUrlShouldHaveUserName() throws Exception {
    ConnectionConfigurator connConf = mock(ConnectionConfigurator.class);
    TimelineReaderPseudoAuthenticatedStrategy.PseudoAuthenticatedURLConnectionFactory
        connectionFactory = new TimelineReaderPseudoAuthenticatedStrategy
          .PseudoAuthenticatedURLConnectionFactory(connConf);
    String inputUrl = "http://host:8080/path";
    String expectedUrl = inputUrl + "?user.name=" + UserGroupInformation.getCurrentUser().getShortUserName();
    HttpURLConnection httpURLConnection = connectionFactory.getHttpURLConnection(new URL(inputUrl));
    Assert.assertEquals(expectedUrl, httpURLConnection.getURL().toString());
  }

}
