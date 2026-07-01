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

import java.io.PrintWriter;

import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;

import com.google.inject.Inject;

public class TezChildWebController extends Controller {

  @Inject
  public TezChildWebController(RequestContext requestContext) {
    super(requestContext);
  }

  @Override
  public void index() {
    ui();
  }

  public void ui() {
    render(StaticTezChildView.class);
  }

  public static class StaticTezChildView extends View {
    @Inject
    private ExecutionContextImpl executionContext;

    @Override
    public void render() {
      response().setContentType(MimeType.HTML);
      PrintWriter pw = writer();
      pw.write("<html><head><meta charset=\\\"utf-8\\\"><title>TezChild UI</title>");
      pw.write("</head><body>");
      pw.write(String.format("<h1>TezChild UI</h1> <h2>%s, %s</h2> %s :: %s :: %s", executionContext.getHostName(),
          executionContext.getContainerId(), getLink("jmx"), getLink("conf"), getLink("stacks")));
      pw.write("</body></html>");
      pw.flush();
    }

    private String getLink(String path) {
      return "<a href=\"/" + path + "\">" + path + "</a>";
    }
  }
}
