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

package org.apache.tez.tools.javadoc.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.tez.dag.api.TezException;
import org.apache.tez.tools.javadoc.model.Config;
import org.apache.tez.tools.javadoc.model.ConfigProperty;

public class HtmlWriter extends Writer {

  private static final String DEFAULT_STYLESHEET = "default-stylesheet.css";

  public void write(Config config) throws IOException {
    PrintWriter out = null;

    if (config.configName == null || config.configName.isEmpty()) {
      throw new RuntimeException("Config Name is null or empty");
    }

    try {
      File file = new File(config.configName + ".html");
      out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"));

      out.println("<?xml version=\"1.0\" encoding=\"ISO-8859-1\" standalone=\"no\" ?>");
      out.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\"");
      out.println("    \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">");

      out.println("<html xmlns=\"http://www.w3.org/1999/xhtml\">");

      out.println("<head>");
      out.println("<meta http-equiv=\"Content-Type\" content=\"text/html; charset=ISO-8859-1\" />");
      out.println("<title>"+ config.configName +"</title>");
//      out.println("<link rel='stylesheet' type='text/css' href=' " + config.getStyleSheet() + "'/>");
      out.println("</head>");

      out.println("<style>");
      out.println("table");
      out.println("{");
      out.println("  background-color: #000;");
      out.println("  border-spacing: 1px;");
      out.println("  margin: 0 auto 0 auto;");
      out.println("}");
      out.println("th");
      out.println("{");
      out.println("  background-color: #fff;");
      out.println("  padding: 5px;");
      out.println("  margin: 1px;");
      out.println("}");
      out.println("td");
      out.println("{");
      out.println("  background-color: #fff;");
      out.println("  padding: 2px;");
      out.println("}");
      out.println("table, th, td {");
      out.println("  border: 1px solid black;");
      out.println("}");
      out.println("tr.tr_private td");
      out.println("{");
      out.println("  background-color: #FF4500;");
      out.println("  padding: 2px;");
      out.println("}");
      out.println("tr.tr_evolve_unstable td");
      out.println("{");
      out.println("  background-color: #FFFFE0;");
      out.println("  padding: 2px;");
      out.println("}");
      out.println("th.th_private");
      out.println("{");
      out.println("  background-color: #FF4500;");
      out.println("}");
      out.println("th.th_evolve_unstable");
      out.println("{");
      out.println("  background-color: #FFFFE0;");
      out.println("}");

      out.println("</style>");

      out.println("<body>");

      out.println("<div id=\"wrapper\">");
      out.println("<div id=\"container\">");

      out.println("<h1>"+ config.configName +"</h1>");
      out.println("<hr />");

      out.println("<table>");
      out.println("<tr>");
      out.println("<th>" + "Property Name" + "</th>");
      out.println("<th>" + "Default Value" + "</th>");
      out.println("<th>" + "Description" + "</th>");
      out.println("<th>" + "Type" + "</th>");
      // out.println("<th>" + "Valid Values" + "</th>");
      out.println("<th class=\"th_private\">" + "Is Private?" + "</th>");
      out.println("<th class=\"th_evolve_unstable\">" + "Is Unstable?" + "</th>");
      out.println("<th class=\"th_evolve_unstable\">" + "Is Evolving?" + "</th>");
      out.println("</tr>");

      for (ConfigProperty configProperty : config.configProperties.values()) {
        if (!isValidConfigProperty(configProperty)) {
          continue;
        }

        String altClass = "";
        if (configProperty.isPrivate) {
          altClass = "class=\"tr_private\"";
        } else if (configProperty.isEvolving || configProperty.isUnstable) {
          altClass = "class=\"tr_evolve_unstable\"";
        }

        out.println("<tr " + altClass + ">");
        out.println("<td>" + configProperty.propertyName + "</td>");
        out.println("<td>" + configProperty.defaultValue + "</td>");
        out.println("<td>" + configProperty.description + "</td>");
        out.println("<td>" + configProperty.type + "</td>");
        // Re-enable after adding values
        // out.println("<td>" + configProperty.validValues + "</td>");

        out.println("<td class=\"" + (configProperty.isPrivate? "td_private_true" : "td_private_false") + "\">" + configProperty.isPrivate + "</td>");
        out.println("<td class=\"" + (configProperty.isEvolving? "td_evolve_true" : "td_evolve_false") + "\">" + configProperty.isEvolving + "</td>");
        out.println("<td class=\"" + (configProperty.isUnstable? "td_unstable_true" : "td_unstable_false") + "\">" + configProperty.isUnstable + "</td>");
        out.println("</tr>");
      }

      out.println("</table>");

      out.println("</div>");
      out.println("</div>");
      out.println("</body>");
      out.println("</html>");

    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

}
