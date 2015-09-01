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

package org.apache.tez.tools.javadoc.doclet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.common.annotation.ConfigurationClass;
import org.apache.tez.common.annotation.ConfigurationProperty;
import org.apache.tez.tools.javadoc.model.Config;
import org.apache.tez.tools.javadoc.model.ConfigProperty;
import org.apache.tez.tools.javadoc.util.HtmlWriter;
import org.apache.tez.tools.javadoc.util.XmlWriter;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.AnnotationDesc.ElementValuePair;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.FieldDoc;
import com.sun.javadoc.LanguageVersion;
import com.sun.javadoc.RootDoc;
import com.sun.tools.doclets.standard.Standard;

public class ConfigStandardDoclet {

  private static final String DEBUG_SWITCH = "-debug";
  private static boolean debugMode = false;

  public static LanguageVersion languageVersion() {
    return LanguageVersion.JAVA_1_5;
  }

  private static void logMessage(String message) {
    if (!debugMode) {
      return;
    }
    System.out.println(message);
  }

  public static boolean start(RootDoc root) {
    //look for debug flag
    for (String[] opts : root.options()) {
      for (String opt : opts) {
        if (opt.equals(DEBUG_SWITCH)) {
          debugMode = true;
        }
      }
    }

    logMessage("Running doclet " + ConfigStandardDoclet.class.getSimpleName());
    ClassDoc[] classes = root.classes();
    for (int i = 0; i < classes.length; ++i) {
      processDoc(classes[i]);
    }

    return true;
  }

  private static void processDoc(ClassDoc doc) {
    logMessage("Parsing : " + doc);
    if (!doc.isClass()) {
      logMessage("Ignoring non-class: " + doc);
      return;
    }

    AnnotationDesc[] annotations = doc.annotations();
    boolean isConfigClass = false;
    String templateName = null;
    for (AnnotationDesc annotation : annotations) {
      logMessage("Checking annotation: " + annotation.annotationType());
      if (annotation.annotationType().qualifiedTypeName().equals(
          ConfigurationClass.class.getName())) {
        isConfigClass = true;
        for (ElementValuePair element : annotation.elementValues()) {
          if (element.element().name().equals("templateFileName")) {
            templateName = stripQuotes(element.value().toString());
          }
        }
        break;
      }
    }

    if (!isConfigClass) {
      logMessage("Ignoring non-config class: " + doc);
      return;
    }

    logMessage("Processing config class: " + doc);
    Config config = new Config(doc.name(), templateName);
    Map<String, ConfigProperty> configProperties = config.configProperties;

    FieldDoc[] fields = doc.fields();
    for (FieldDoc field : fields) {
      if (field.isPrivate()) {
        logMessage("Skipping private field: " + field);
        continue;
      }
      if (!field.isStatic()) {
        logMessage("Skipping non-static field: " + field);
        continue;
      }

      if (field.name().endsWith("_PREFIX")) {
        logMessage("Skipping non-config prefix constant field: " + field);
        continue;
      }
      if (field.name().equals("TEZ_SITE_XML")) {
        logMessage("Skipping constant field: " + field);
        continue;
      }

      if (field.name().endsWith("_DEFAULT")) {

        String name = field.name().substring(0,
            field.name().lastIndexOf("_DEFAULT"));
        if (!configProperties.containsKey(name)) {
          configProperties.put(name, new ConfigProperty());
        }
        ConfigProperty configProperty = configProperties.get(name);
        if (field.constantValue() == null) {
          logMessage("Got null constant value"
              + ", name=" + name
              + ", field=" + field.name()
              + ", val=" + field.constantValueExpression());
          configProperty.defaultValue = field.constantValueExpression();
        } else {
          configProperty.defaultValue = field.constantValue().toString();
        }
        configProperty.inferredType = field.type().simpleTypeName();

        if (name.equals("TEZ_AM_STAGING_DIR") && configProperty.defaultValue != null) {
          String defaultValue = configProperty.defaultValue;
          defaultValue = defaultValue.replace(System.getProperty("user.name"), "${user.name}");
          configProperty.defaultValue = defaultValue;
        }

        continue;
      }

      String name = field.name();
      if (!configProperties.containsKey(name)) {
        configProperties.put(name, new ConfigProperty());
      }
      ConfigProperty configProperty = configProperties.get(name);
      configProperty.propertyName = field.constantValue().toString();

      AnnotationDesc[] annotationDescs = field.annotations();

      for (AnnotationDesc annotationDesc : annotationDescs) {

        if (annotationDesc.annotationType().qualifiedTypeName().equals(
            Private.class.getCanonicalName())) {
          configProperty.isPrivate = true;
        }
        if (annotationDesc.annotationType().qualifiedTypeName().equals(
            Unstable.class.getCanonicalName())) {
          configProperty.isUnstable = true;
        }
        if (annotationDesc.annotationType().qualifiedTypeName().equals(
            Evolving.class.getCanonicalName())) {
          configProperty.isEvolving = true;
        }
        if (annotationDesc.annotationType().qualifiedTypeName().equals(
            ConfigurationProperty.class.getCanonicalName())) {
          configProperty.isValidConfigProp = true;

          boolean foundType = false;
          for (ElementValuePair element : annotationDesc.elementValues()) {
            if (element.element().name().equals("type")) {
              configProperty.type = stripQuotes(element.value().toString());
              foundType = true;
            } else {
              logMessage("Unhandled annotation property: " + element.element().name());
            }
          }
        }
      }

      configProperty.description = field.commentText();

    }

    HtmlWriter writer = new HtmlWriter();
    try {
      writer.write(config);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    XmlWriter xmlWriter = new XmlWriter();
    try {
      xmlWriter.write(config);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  private static String stripQuotes(String s) {
    if (s.charAt(0) == '"' && s.charAt(s.length()-1) == '"') {
      return s.substring(1, s.length()-1);
    }
    return s;
  }

  public static int optionLength(String option) {
    return Standard.optionLength(option);
  }

  public static boolean validOptions(String options[][], DocErrorReporter reporter) {
    return true;
  }
}
