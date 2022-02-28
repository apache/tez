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

package org.apache.tez.tools.javadoc.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.tez.tools.javadoc.model.Config;
import org.apache.tez.tools.javadoc.model.ConfigProperty;

import com.google.common.io.ByteStreams;

public class XmlWriter extends Writer {

  public void write(Config config) throws IOException {
    PrintWriter out = null;

    if (config.getConfigName() == null || config.getConfigName().isEmpty()) {
      throw new RuntimeException("Config Name is null or empty");
    }

    String fileName = config.getTemplateName() == null ||
        config.getTemplateName().isEmpty() ? config.getConfigName() : config.getTemplateName();
    if (!fileName.endsWith(".xml")) {
      fileName += ".xml";
    }

    try {
      File file = new File(fileName);
      writeApacheHeader(file);

      out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));

      out.println("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>");
      out.println();
      out.println("<!-- WARNING: THIS IS A GENERATED TEMPLATE PURELY FOR DOCUMENTATION PURPOSES");
      out.println(" AND SHOULD NOT BE USED AS A CONFIGURATION FILE FOR TEZ -->");
      out.println();
      out.println("<configuration>");

      for (ConfigProperty configProperty : config.getConfigProperties().values()) {
        if (!isValidConfigProperty(configProperty)) {
          continue;
        }
        out.println();
        out.println("  <property>");
        out.println("    <name>" + configProperty.getPropertyName() + "</name>");
        if (configProperty.getDefaultValue() != null && !configProperty.getDefaultValue().isEmpty()) {
          out.println("    <defaultValue>" + configProperty.getDefaultValue() + "</defaultValue>");
        }
        if (configProperty.getDescription() != null && !configProperty.getDescription().isEmpty()) {
          out.println("    <description>"
              + StringEscapeUtils.escapeXml(configProperty.getDescription()) + "</description>");
        }
        if (configProperty.getType() != null && !configProperty.getType().isEmpty()) {
          out.println("    <type>" + configProperty.getType() + "</type>");
        }
        if (configProperty.isUnstable()) {
          out.println("    <unstable>true</unstable>");
        }
        if (configProperty.isEvolving()) {
          out.println("    <evolving>true</evolving>");
        }
        if (configProperty.isPrivate()) {
          out.println("    <private>true</private>");
        }
        out.println("  </property>");
      }

      out.println();
      out.println("</configuration>");
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  private void writeApacheHeader(File file) throws IOException {
    try (InputStream in = this.getClass().getClassLoader().getResourceAsStream("apache-licence.xml.header");
         OutputStream out = new FileOutputStream(file)) {
      ByteStreams.copy(in, out);
    }
  }
}
