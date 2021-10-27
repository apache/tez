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

package org.apache.tez.common.web;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.yarn.webapp.Controller;

/**
 * AbstractServletToControllerAdapter is a common ancestor for classes
 * that wish to adapt servlets to yarn webapp controllers.
 * The adapter is responsible for:
 * 1. creating a servlet instance
 * 2. creating a dummy ServletConfig
 * 3. delegating calls to the servlet instance's doGet method
 */
public abstract class AbstractServletToControllerAdapter extends Controller {
  private AtomicBoolean initialized = new AtomicBoolean(false);
  protected HttpServlet servlet;

  @Override
  public void index() {
    if (initialized.compareAndSet(false, true)) {
      initServlet();
    }
    try {
      /*
       * This reflection workaround is needed because HttpServlet.doGet is protected
       * (even if subclasses have it public).
       */
      Method doGetMethod =
          this.servlet.getClass().getMethod("doGet", HttpServletRequest.class, HttpServletResponse.class);
      doGetMethod.setAccessible(true);
      doGetMethod.invoke(this.servlet, request(), response());
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
        | SecurityException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a dummy servlet config which is suitable for initializing a servlet instance.
   * @param servletName
   * @return a ServletConfig instance initialized with a ServletContext
   */
  private ServletConfig getDummyServletConfig(String servletName) {
    return new ServletConfig() {

      @Override
      public String getServletName() {
        return servletName;
      }

      @Override
      public ServletContext getServletContext() {
        return request().getServletContext();
      }

      @Override
      public Enumeration<String> getInitParameterNames() {
        return null;
      }

      @Override
      public String getInitParameter(String name) {
        return null;
      }
    };
  }

  private void initServlet() {
    try {
      servlet.init(getDummyServletConfig(this.servlet.getClass().getSimpleName()));
    } catch (ServletException e) {
      throw new RuntimeException(e);
    }
  }
}
