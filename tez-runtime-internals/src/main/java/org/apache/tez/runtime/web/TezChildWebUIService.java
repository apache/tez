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
package org.apache.tez.runtime.web;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.tez.common.web.ServletToControllerAdapters.ConfServletController;
import org.apache.tez.common.web.ServletToControllerAdapters.JMXJsonServletController;
import org.apache.tez.common.web.ServletToControllerAdapters.StackServletController;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezChildWebUIService {
  private static final Logger LOG = LoggerFactory.getLogger(TezChildWebUIService.class);

  private Configuration conf;
  private ExecutionContext executionContext;
  private TezChildWebApp tezChildWebApp;
  private WebApp webApp;
  private String baseUrl = ""; //url without paths, like http://host:port

  public TezChildWebUIService(Configuration conf, ExecutionContext executionContext) {
    this.tezChildWebApp = new TezChildWebApp(executionContext);
    this.conf = conf;
    this.executionContext = executionContext;
  }

  public TezChildWebUIService start() {
    try {
      if (conf.get(TezConfiguration.TEZ_TASK_WEBSERVICE_PORT_RANGE) == null) {
        conf.set(TezConfiguration.TEZ_TASK_WEBSERVICE_PORT_RANGE,
            TezConfiguration.TEZ_TASK_WEBSERVICE_PORT_RANGE_DEFAULT);
        LOG.info(
            "Using default port range for WebUIService: " + conf.get(TezConfiguration.TEZ_TASK_WEBSERVICE_PORT_RANGE));
      }
      this.webApp = WebApps.$for(this.tezChildWebApp).with(conf)
          .withPortRange(conf, TezConfiguration.TEZ_TASK_WEBSERVICE_PORT_RANGE).start(this.tezChildWebApp);
      InetSocketAddress address = webApp.getListenerAddress();
      if (address != null) {
        String hostname = executionContext.getHostName();
        InetSocketAddress bindAddress = NetUtils.createSocketAddrForHost(hostname, address.getPort());
        final int port = address.getPort();
        if (bindAddress.getAddress() != null && bindAddress.getAddress().getCanonicalHostName() != null) {
          hostname = bindAddress.getAddress().getCanonicalHostName();
        } else {
          LOG.warn("Failed to resolve canonical hostname for " + hostname);
        }
        baseUrl = String.format("http://%s:%d", hostname, port);
        LOG.info("Instantiated TezChild WebUIService at " + baseUrl + "/ui");
      }
    } catch (Exception e) {
      LOG.error("TezChild WebUIService failed to start.", e);
      throw new TezUncheckedException(e);
    }
    return this;
  }

  public void stop() {
    if (this.webApp != null) {
      LOG.debug("Stopping WebApp");
      this.webApp.stop();
    }
  }

  private static class TezChildWebApp extends WebApp {
    private ExecutionContext executionContext;

    TezChildWebApp(ExecutionContext executionContext) {
      this.executionContext = executionContext;
    }

    @Override
    public void setup() {
      bind(ExecutionContextImpl.class).toInstance((ExecutionContextImpl) executionContext);
      route("/", TezChildWebController.class, "ui");
      route("/ui", TezChildWebController.class, "ui");
      route("/jmx", JMXJsonServletController.class);
      route("/conf", ConfServletController.class);
      route("/stacks", StackServletController.class);
    }
  }
}
