/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.http;

import org.asynchttpclient.DefaultAsyncHttpClientConfig;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory;
import org.apache.hadoop.security.ssl.KeyStoresFactory;
import org.apache.hadoop.security.ssl.SSLFactory.Mode;
import org.apache.hadoop.security.ssl.SSLHostnameVerifier;
import org.apache.hadoop.util.ReflectionUtils;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.security.GeneralSecurityException;
import java.util.Objects;

import static org.apache.hadoop.security.ssl.SSLFactory.KEYSTORES_FACTORY_CLASS_KEY;
import static org.apache.hadoop.security.ssl.SSLFactory.SSL_CLIENT_CONF_KEY;
import static org.apache.hadoop.security.ssl.SSLFactory.SSL_HOSTNAME_VERIFIER_KEY;
import static org.apache.hadoop.security.ssl.SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY;
import static org.apache.hadoop.security.ssl.SSLFactory.SSL_SERVER_CONF_KEY;

/**
 * Factory that creates SSLEngine and SSLSocketFactory instances using
 * Hadoop configuration information.
 * <p/>
 * This SSLFactory uses a {@link org.apache.hadoop.security.ssl.ReloadingX509TrustManager} instance,
 * which reloads public keys if the truststore file changes.
 * <p/>
 * This factory is used to configure HTTPS in Hadoop HTTP based endpoints, both
 * client and server.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SSLFactory implements ConnectionConfigurator {

  public static final String SSL_ENABLED_PROTOCOLS = "hadoop.ssl.enabled.protocols";
  public static final String DEFAULT_SSL_ENABLED_PROTOCOLS = "TLSv1";
  public static final boolean DEFAULT_SSL_REQUIRE_CLIENT_CERT = false;

  private Configuration conf;
  private Mode mode;
  private boolean requireClientCert;
  private SSLContext context;
  private HostnameVerifier hostnameVerifier;
  private KeyStoresFactory keystoresFactory;

  private String[] enabledProtocols = null;

  /**
   * Creates an SSLFactory.
   *
   * @param mode SSLFactory mode, client or server.
   * @param conf Hadoop configuration from where the SSLFactory configuration
   *             will be read.
   * @throws NullPointerException if {@code mode} or {@code conf} is {@code null}
   */
  public SSLFactory(Mode mode, Configuration conf) {
    this.conf = Objects.requireNonNull(conf);
    this.mode = Objects.requireNonNull(mode, "mode cannot be NULL");
    requireClientCert = conf.getBoolean(SSL_REQUIRE_CLIENT_CERT_KEY,
        DEFAULT_SSL_REQUIRE_CLIENT_CERT);
    // Rest of ssl configs are pre-populated in incoming conf payload
    conf.setBoolean(SSL_REQUIRE_CLIENT_CERT_KEY, requireClientCert);

    Class<? extends KeyStoresFactory> klass
        = conf.getClass(KEYSTORES_FACTORY_CLASS_KEY,
        FileBasedKeyStoresFactory.class, KeyStoresFactory.class);
    keystoresFactory = ReflectionUtils.newInstance(klass, conf);
    enabledProtocols = conf.getStrings(SSL_ENABLED_PROTOCOLS, DEFAULT_SSL_ENABLED_PROTOCOLS);
  }

  private Configuration readSSLConfiguration(Mode mode) {
    Configuration sslConf = new Configuration(false);
    sslConf.setBoolean(SSL_REQUIRE_CLIENT_CERT_KEY, requireClientCert);
    String sslConfResource;
    if (mode == Mode.CLIENT) {
      sslConfResource = conf.get(SSL_CLIENT_CONF_KEY, "ssl-client.xml");
    } else {
      sslConfResource = conf.get(SSL_SERVER_CONF_KEY, "ssl-server.xml");
    }
    sslConf.addResource(sslConfResource);
    return sslConf;
  }

  /**
   * Initializes the factory.
   *
   * @throws GeneralSecurityException thrown if an SSL initialization error
   *                                  happened.
   * @throws IOException              thrown if an IO error happened while reading the SSL
   *                                  configuration.
   */
  public void init() throws GeneralSecurityException, IOException {
    keystoresFactory.init(mode);
    context = SSLContext.getInstance("TLS");
    context.init(keystoresFactory.getKeyManagers(),
        keystoresFactory.getTrustManagers(), null);
    context.getDefaultSSLParameters().setProtocols(enabledProtocols);
    hostnameVerifier = getHostnameVerifier(conf);
  }

  private HostnameVerifier getHostnameVerifier(Configuration conf)
      throws GeneralSecurityException, IOException {
    return getHostnameVerifier(conf.get(SSL_HOSTNAME_VERIFIER_KEY, "DEFAULT").
        trim().toUpperCase());
  }

  public static HostnameVerifier getHostnameVerifier(String verifier)
      throws GeneralSecurityException, IOException {
    HostnameVerifier hostnameVerifier;
    if (verifier.equals("DEFAULT")) {
      hostnameVerifier = SSLHostnameVerifier.DEFAULT;
    } else if (verifier.equals("DEFAULT_AND_LOCALHOST")) {
      hostnameVerifier = SSLHostnameVerifier.DEFAULT_AND_LOCALHOST;
    } else if (verifier.equals("STRICT")) {
      hostnameVerifier = SSLHostnameVerifier.STRICT;
    } else if (verifier.equals("STRICT_IE6")) {
      hostnameVerifier = SSLHostnameVerifier.STRICT_IE6;
    } else if (verifier.equals("ALLOW_ALL")) {
      hostnameVerifier = SSLHostnameVerifier.ALLOW_ALL;
    } else {
      throw new GeneralSecurityException("Invalid hostname verifier: " +
          verifier);
    }
    return hostnameVerifier;
  }

  /**
   * Releases any resources being used.
   */
  public void destroy() {
    keystoresFactory.destroy();
  }

  /**
   * Returns the SSLFactory KeyStoresFactory instance.
   *
   * @return the SSLFactory KeyStoresFactory instance.
   */
  public KeyStoresFactory getKeystoresFactory() {
    return keystoresFactory;
  }


  /**
   * Returns a configured SSLSocketFactory.
   *
   * @return the configured SSLSocketFactory.
   * @throws GeneralSecurityException thrown if the SSLSocketFactory could not
   *                                  be initialized.
   * @throws IOException              thrown if and IO error occurred while loading
   *                                  the server keystore.
   */
  public SSLSocketFactory createSSLSocketFactory() throws GeneralSecurityException, IOException {
    if (mode != Mode.CLIENT) {
      throw new IllegalStateException("Factory is in CLIENT mode");
    }
    return context.getSocketFactory();
  }

  /**
   * Returns the hostname verifier it should be used in HttpsURLConnections.
   *
   * @return the hostname verifier.
   */
  public HostnameVerifier getHostnameVerifier() {
    if (mode != Mode.CLIENT) {
      throw new IllegalStateException("Factory is in CLIENT mode");
    }
    return hostnameVerifier;
  }



  /**
   * If the given {@link HttpURLConnection} is an {@link HttpsURLConnection}
   * configures the connection with the {@link SSLSocketFactory} and
   * {@link HostnameVerifier} of this SSLFactory, otherwise does nothing.
   *
   * @param conn the {@link HttpURLConnection} instance to configure.
   * @return the configured {@link HttpURLConnection} instance.
   * @throws IOException if an IO error occurred.
   */
  @Override
  public HttpURLConnection configure(HttpURLConnection conn) throws IOException {
    if (conn instanceof HttpsURLConnection) {
      HttpsURLConnection sslConn = (HttpsURLConnection) conn;
      try {
        sslConn.setSSLSocketFactory(createSSLSocketFactory());
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }
      sslConn.setHostnameVerifier(getHostnameVerifier());
      conn = sslConn;
    }
    return conn;
  }

  /**
   * Set ssl context for {@link org.asynchttpclient.DefaultAsyncHttpClientConfig.Builder}
   *
   * @param builder {@link org.asynchttpclient.DefaultAsyncHttpClientConfig.Builder} instance to
   *                configure.
   * @throws IOException if an IO error occurred.
   */
  public void configure(DefaultAsyncHttpClientConfig.Builder builder) throws IOException {
    if (builder != null) {
      JdkSslContext jdkSslContext =
          new JdkSslContext(context, mode.equals(Mode.CLIENT), /* ciphers */null,
              SupportedCipherSuiteFilter.INSTANCE, /* ApplicationProtocolConfig */ null,
              requireClientCert ? ClientAuth.REQUIRE : ClientAuth.OPTIONAL, enabledProtocols,
              /* startTls */ true);
      builder.setSslContext(jdkSslContext);
    }
  }
}