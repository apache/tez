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

import static org.apache.hadoop.security.ssl.SSLFactory.Mode.CLIENT;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.GeneralSecurityException;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HttpURLConnectionFactory;
import com.sun.jersey.client.urlconnection.URLConnectionClientHandler;
import com.sun.jersey.json.impl.provider.entity.JSONRootElementProvider;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 *  TimelineReaderFactory getTimelineReaderStrategy returns a Strategy class, which is used to
 *  create a httpclient, configured for the appropriate runtime.
 *
 *  on hadoop 2.6+ the factory returns TimelineReaderTokenAuthenticatedStrategy, which supports
 *  kerberos based auth (secure cluster) or psuedo auth (un-secure cluster).
 *
 *  on hadoop 2.4 where the token delegation auth is not supported, TimelineReaderPseudoAuthenticatedStrategy
 *  is used which supports only unsecure timeline.
 *
 */
@InterfaceAudience.Private
public class TimelineReaderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TimelineReaderFactory.class);

  private static final String KERBEROS_DELEGATION_TOKEN_AUTHENTICATOR_CLAZZ_NAME =
      "org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticator";
  private static final String PSEUDO_DELEGATION_TOKEN_AUTHENTICATOR_CLAZZ_NAME =
      "org.apache.hadoop.security.token.delegation.web.PseudoDelegationTokenAuthenticator";
  private static final String DELEGATION_TOKEN_AUTHENTICATED_URL_CLAZZ_NAME =
      "org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL";
  private static final String DELEGATION_TOKEN_AUTHENTICATOR_CLAZZ_NAME =
      "org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator";
  private static final String DELEGATION_TOKEN_AUTHENTICATED_URL_TOKEN_CLASS_NAME =
      "org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL$Token";

  private static Class<?> delegationTokenAuthenticatorClazz = null;
  private static Method delegationTokenAuthenticateURLOpenConnectionMethod = null;

  public static TimelineReaderStrategy getTimelineReaderStrategy(Configuration conf,
                                                                 boolean useHttps,
                                                                 int connTimeout) throws TezException {

    TimelineReaderStrategy timelineReaderStrategy;

    if (!isTimelineClientSupported()) {
      throw new TezException("Reading from timeline is not supported." +
          " token delegation support: " + tokenDelegationSupported() +
          ", is secure timeline: " + UserGroupInformation.isSecurityEnabled());
    }

    timelineReaderStrategy = getTimelineReaderStrategy(tokenDelegationSupported(), conf, useHttps,
        connTimeout);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Using " + timelineReaderStrategy.getClass().getName() + " to read timeline data");
    }

    return timelineReaderStrategy;
  }

  private static TimelineReaderStrategy getTimelineReaderStrategy(boolean isTokenDelegationSupported,
                                                                    Configuration conf,
                                                                    boolean useHttps,
                                                                    int connTimeout) {
    TimelineReaderStrategy timelineReaderStrategy;

    if (isTokenDelegationSupported) {
      timelineReaderStrategy =
          new TimelineReaderTokenAuthenticatedStrategy(conf, useHttps, connTimeout);
    } else {
      timelineReaderStrategy =
          new TimelineReaderPseudoAuthenticatedStrategy(conf, useHttps, connTimeout);
    }

    return timelineReaderStrategy;
  }

  /**
   * Check if timeline client can be supported.
   *
   * @return boolean value indicating if timeline client to read data is supported.
   */
  public static boolean isTimelineClientSupported() {
    // support to read data from timeline is based on the version of hadoop.
    // reads are supported for non-secure cluster from hadoop 2.4 and up.
    // reads are supported for secure cluster only from hadoop 2.6. check the presence of the classes
    // required upfront if security is enabled.
    return !UserGroupInformation.isSecurityEnabled() || tokenDelegationSupported();
  }

  public interface TimelineReaderStrategy {
    Client getHttpClient() throws IOException;
  }

  /*
   * auth strategy for secured and unsecured environment with delegation token (hadoop 2.6 and above)
   */
  private static class TimelineReaderTokenAuthenticatedStrategy implements TimelineReaderStrategy {
    private final Configuration conf;
    private final boolean useHttps;
    private final int connTimeout;

    public TimelineReaderTokenAuthenticatedStrategy(final Configuration conf,
                                                    final boolean useHttps,
                                                    final int connTimeout) {

      this.conf = conf;
      this.useHttps = useHttps;
      this.connTimeout = connTimeout;
    }

    @Override
    public Client getHttpClient() throws IOException {
      Authenticator authenticator;
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      UserGroupInformation realUgi = ugi.getRealUser();
      UserGroupInformation authUgi;
      String doAsUser;
      ClientConfig clientConfig = new DefaultClientConfig(JSONRootElementProvider.App.class);
      ConnectionConfigurator connectionConfigurator = getNewConnectionConf(conf, useHttps,
          connTimeout);

      try {
        authenticator = getTokenAuthenticator();
        authenticator.setConnectionConfigurator(connectionConfigurator);
      } catch (TezException e) {
        throw new IOException("Failed to get authenticator", e);
      }

      if (realUgi != null) {
        authUgi = realUgi;
        doAsUser = ugi.getShortUserName();
      } else {
        authUgi = ugi;
        doAsUser = null;
      }

      HttpURLConnectionFactory connectionFactory;
      try {
        connectionFactory = new TokenAuthenticatedURLConnectionFactory(connectionConfigurator, authenticator,
            authUgi, doAsUser);
      } catch (TezException e) {
        throw new IOException("Fail to create TokenAuthenticatedURLConnectionFactory", e);
      }
      return new Client(new URLConnectionClientHandler(connectionFactory), clientConfig);
    }

    private static Authenticator getTokenAuthenticator() throws TezException {
      String authenticatorClazzName;

      if (UserGroupInformation.isSecurityEnabled()) {
        authenticatorClazzName = KERBEROS_DELEGATION_TOKEN_AUTHENTICATOR_CLAZZ_NAME;
      } else {
        authenticatorClazzName = PSEUDO_DELEGATION_TOKEN_AUTHENTICATOR_CLAZZ_NAME;
      }

      return ReflectionUtils.createClazzInstance(authenticatorClazzName);
    }

    private static class TokenAuthenticatedURLConnectionFactory implements HttpURLConnectionFactory {

      private final Authenticator authenticator;
      private final ConnectionConfigurator connConfigurator;
      private final UserGroupInformation authUgi;
      private final String doAsUser;
      private final AuthenticatedURL.Token token;

      public TokenAuthenticatedURLConnectionFactory(ConnectionConfigurator connConfigurator,
                                                    Authenticator authenticator,
                                                    UserGroupInformation authUgi,
                                                    String doAsUser) throws TezException {
        this.connConfigurator = connConfigurator;
        this.authenticator = authenticator;
        this.authUgi = authUgi;
        this.doAsUser = doAsUser;
        this.token = ReflectionUtils.createClazzInstance(
            DELEGATION_TOKEN_AUTHENTICATED_URL_TOKEN_CLASS_NAME, null, null);
      }

      @Override
      public HttpURLConnection getHttpURLConnection(URL url) throws IOException {
        try {
          AuthenticatedURL authenticatedURL= ReflectionUtils.createClazzInstance(
              DELEGATION_TOKEN_AUTHENTICATED_URL_CLAZZ_NAME, new Class[] {
              delegationTokenAuthenticatorClazz,
              ConnectionConfigurator.class
          }, new Object[] {
              authenticator,
              connConfigurator
          });
          return ReflectionUtils.invokeMethod(authenticatedURL,
              delegationTokenAuthenticateURLOpenConnectionMethod, url, token, doAsUser);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
  }

  /*
   * Pseudo auth strategy for env where delegation token is not supported (hadoop 2.4)
   */
  @VisibleForTesting
  protected static class TimelineReaderPseudoAuthenticatedStrategy implements TimelineReaderStrategy {

    private final ConnectionConfigurator connectionConf;

    public TimelineReaderPseudoAuthenticatedStrategy(final Configuration conf,
                                                     final boolean useHttps,
                                                     final int connTimeout) {
      connectionConf = getNewConnectionConf(conf, useHttps, connTimeout);
    }

    @Override
    public Client getHttpClient() {
      ClientConfig config = new DefaultClientConfig(JSONRootElementProvider.App.class);
      HttpURLConnectionFactory urlFactory = new PseudoAuthenticatedURLConnectionFactory(connectionConf);
      Client httpClient = new Client(new URLConnectionClientHandler(urlFactory), config);
      return httpClient;
    }

    @VisibleForTesting
    protected static class PseudoAuthenticatedURLConnectionFactory implements HttpURLConnectionFactory {
      private final ConnectionConfigurator connectionConf;

      public PseudoAuthenticatedURLConnectionFactory(ConnectionConfigurator connectionConf) {
        this.connectionConf = connectionConf;
      }

      @Override
      public HttpURLConnection getHttpURLConnection(URL url) throws IOException {
        String tokenString = (url.getQuery() == null ? "?" : "&") + "user.name=" +
            URLEncoder.encode(UserGroupInformation.getCurrentUser().getShortUserName(), "UTF8");

        HttpURLConnection httpURLConnection =
            (HttpURLConnection) (new URL(url.toString() + tokenString)).openConnection();
        this.connectionConf.configure(httpURLConnection);

        return httpURLConnection;
      }
    }
  }

  private static ConnectionConfigurator getNewConnectionConf(final Configuration conf,
                                                             final boolean useHttps,
                                                             final int connTimeout) {
    ConnectionConfigurator connectionConf = null;
    if (useHttps) {
      try {
        connectionConf = getNewSSLConnectionConf(conf, connTimeout);
      } catch (IOException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cannot load customized ssl related configuration."
              + " Falling back to system-generic settings.", e);
        }
      }
    }

    if (connectionConf == null) {
      connectionConf = new ConnectionConfigurator() {
        @Override
        public HttpURLConnection configure(HttpURLConnection httpURLConnection) throws IOException {
          setTimeouts(httpURLConnection, connTimeout);
          return httpURLConnection;
        }
      };
    }

    return connectionConf;
  }

  private static ConnectionConfigurator getNewSSLConnectionConf(final Configuration conf,
                                                                final int connTimeout)
      throws IOException {
    final SSLFactory sslFactory;
    final SSLSocketFactory sslSocketFactory;
    final HostnameVerifier hostnameVerifier;

    sslFactory = new SSLFactory(CLIENT, conf);
    try {
      sslFactory.init();
      sslSocketFactory = sslFactory.createSSLSocketFactory();
    } catch (GeneralSecurityException e) {
      sslFactory.destroy();
      throw new IOException("Failed to initialize ssl factory");
    }
    hostnameVerifier = sslFactory.getHostnameVerifier();

    return new ConnectionConfigurator() {
      @Override
      public HttpURLConnection configure(HttpURLConnection httpURLConnection) throws IOException {
        if (!(httpURLConnection instanceof HttpsURLConnection)) {
          throw new IOException("Expected https connection");
        }
        HttpsURLConnection httpsURLConnection = (HttpsURLConnection) httpURLConnection;
        httpsURLConnection.setSSLSocketFactory(sslSocketFactory);
        httpsURLConnection.setHostnameVerifier(hostnameVerifier);
        setTimeouts(httpsURLConnection, connTimeout);

        return httpsURLConnection;
      }
    };
  }

  private static void setTimeouts(HttpURLConnection httpURLConnection, int connTimeout) {
    httpURLConnection.setConnectTimeout(connTimeout);
    httpURLConnection.setReadTimeout(connTimeout);
  }

  private static boolean isTokenDelegationSupportChecksDone = false;
  private static boolean isTokenDelegationClassesPresent = false;

  // Check if all the classes required for doing token authentication are present. These classes
  // are present only from hadoop 2.6 onwards.
  private static synchronized boolean tokenDelegationSupported() {

    if (!isTokenDelegationSupportChecksDone) {

      isTokenDelegationSupportChecksDone = true;

      try {
        ReflectionUtils.getClazz(KERBEROS_DELEGATION_TOKEN_AUTHENTICATOR_CLAZZ_NAME);
        ReflectionUtils.getClazz(PSEUDO_DELEGATION_TOKEN_AUTHENTICATOR_CLAZZ_NAME);

        delegationTokenAuthenticatorClazz =
            ReflectionUtils.getClazz(DELEGATION_TOKEN_AUTHENTICATOR_CLAZZ_NAME);

        Class<?> delegationTokenAuthenticatedURLClazz =
            ReflectionUtils.getClazz(DELEGATION_TOKEN_AUTHENTICATED_URL_CLAZZ_NAME);

        Class<?> delegationTokenAuthenticatedURLTokenClazz =
            ReflectionUtils.getClazz(DELEGATION_TOKEN_AUTHENTICATED_URL_TOKEN_CLASS_NAME);

        delegationTokenAuthenticateURLOpenConnectionMethod =
            ReflectionUtils.getMethod(delegationTokenAuthenticatedURLClazz, "openConnection",
                URL.class, delegationTokenAuthenticatedURLTokenClazz, String.class);

        isTokenDelegationClassesPresent = true;

      } catch (TezException e) {
        LOG.info("Could not find class required for token delegation, will fallback to pseudo auth");
      }
    }

    return isTokenDelegationClassesPresent;
  }
}
