/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.common.web;

import javax.servlet.ServletException;

import org.apache.hadoop.conf.ConfServlet;
import org.apache.hadoop.http.HttpServer2.StackServlet;
import org.apache.hadoop.jmx.JMXJsonServlet;

public class ServletToControllerAdapters {
  public static class JMXJsonServletController extends AbstractServletToControllerAdapter {
    public JMXJsonServletController() throws ServletException {
      this.servlet = new JMXJsonServlet();
    }
  }

  public static class ConfServletController extends AbstractServletToControllerAdapter {
    public ConfServletController() throws ServletException {
      this.servlet = new ConfServlet();
    }
  }

  public static class StackServletController extends AbstractServletToControllerAdapter {
    public StackServletController() throws ServletException {
      this.servlet = new StackServlet();
    }
  }

  public static class ProfileServletController extends AbstractServletToControllerAdapter {
    public ProfileServletController() throws ServletException {
      this.servlet = new ProfileServlet();
    }
  }

  public static class ProfileOutputServletController extends AbstractServletToControllerAdapter {
    public ProfileOutputServletController() throws ServletException {
      this.servlet = new ProfileOutputServlet();
    }
  }

}
