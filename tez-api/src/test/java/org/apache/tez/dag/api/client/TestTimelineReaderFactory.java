/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.dag.api.client;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.TimelineReaderFactory.TimelineReaderPseudoAuthenticatedStrategy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestTimelineReaderFactory {

  // ensure on hadoop 2.6+ TimelineReaderTokenAuthenticatedStrategy is used.
  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testShouldUseTokenDelegationAuthStrategy() throws TezException {

    String returnedClassName =
        TimelineReaderFactory.getTimelineReaderStrategy(mock(Configuration.class), false, 0)
            .getClass()
            .getCanonicalName();
    assertEquals(
        "org.apache.tez.dag.api.client.TimelineReaderFactory.TimelineReaderTokenAuthenticatedStrategy",
        returnedClassName,
        "should use token delegation auth");
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testPseudoAuthenticatorConnectionUrlShouldHaveUserName() throws Exception {
    ConnectionConfigurator connConf = mock(ConnectionConfigurator.class);
    TimelineReaderPseudoAuthenticatedStrategy.PseudoAuthenticatedURLConnectionFactory
        connectionFactory = new TimelineReaderPseudoAuthenticatedStrategy
        .PseudoAuthenticatedURLConnectionFactory(connConf);
    String inputUrl = "http://host:8080/path";
    String expectedUrl = inputUrl + "?user.name=" + UserGroupInformation.getCurrentUser().getShortUserName();
    HttpURLConnection httpURLConnection = connectionFactory.getConnection(URI.create(inputUrl).toURL());
    assertEquals(expectedUrl, httpURLConnection.getURL().toString());
  }

}
