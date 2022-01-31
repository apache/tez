/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.tools.javadoc.doclet;

import com.sun.source.util.DocTrees;
import jdk.javadoc.doclet.Doclet;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.common.annotation.ConfigurationClass;
import org.apache.tez.common.annotation.ConfigurationProperty;
import org.apache.tez.tools.javadoc.model.Config;
import org.apache.tez.tools.javadoc.model.ConfigProperty;
import org.apache.tez.tools.javadoc.util.HtmlWriter;
import org.apache.tez.tools.javadoc.util.XmlWriter;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.*;

public class ConfigStandardDoclet implements Doclet {

  private static boolean debugMode = false;

  private Reporter reporter;

  @Override
  public void init(Locale locale, Reporter reporter) {
    this.reporter = reporter;
  }

  @Override
  public String getName() {
    return "Tez";
  }

  @Override
  public SourceVersion getSupportedSourceVersion() {
    return SourceVersion.RELEASE_9;
  }

  private void logMessage(String message) {
    if (!debugMode) {
      return;
    }
    reporter.print(Diagnostic.Kind.NOTE, message);
  }

  @Override
  public boolean run(DocletEnvironment docEnv) {
    logMessage("Running doclet " + ConfigStandardDoclet.class.getSimpleName());
    DocTrees docTrees = docEnv.getDocTrees();
    for (Element element : docEnv.getIncludedElements()) {
      if (element.getKind().equals(ElementKind.CLASS) && element instanceof TypeElement) {
        processDoc(docTrees, (TypeElement) element);
      }
    }

    return true;
  }

  private void processDoc(DocTrees docTrees, TypeElement doc) {
    logMessage("Parsing : " + doc);
    if (!doc.getKind().equals(ElementKind.CLASS)) {
      logMessage("Ignoring non-class: " + doc);
      return;
    }

    List<? extends AnnotationMirror> annotations = doc.getAnnotationMirrors();
    boolean isConfigClass = false;
    String templateName = null;
    for (AnnotationMirror annotation : annotations) {
      logMessage("Checking annotation: " + annotation.getAnnotationType());
      if (annotation.getAnnotationType().asElement().toString().equals(
              ConfigurationClass.class.getName())) {
        isConfigClass = true;
        Map<? extends ExecutableElement, ? extends AnnotationValue> elementValues = annotation.getElementValues();
        for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> element : elementValues.entrySet()) {
          if (element.getKey().getSimpleName().toString().equals("templateFileName")) {
            templateName = stripQuotes(element.getValue().getValue().toString());
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
    Config config = new Config(doc.getSimpleName().toString(), templateName);
    Map<String, ConfigProperty> configProperties = config.configProperties;

    processElements(docTrees, doc, configProperties);

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

  private void processElements(DocTrees docTrees, TypeElement doc, Map<String, ConfigProperty> configProperties) {
    List<? extends Element> elements = doc.getEnclosedElements();
    for (Element f : elements) {
      if (!(f instanceof VariableElement)) {
        continue;
      }

      if (!f.getKind().equals(ElementKind.FIELD)) {
        continue;
      }

      VariableElement field = (VariableElement) f;

      if (field.getModifiers().contains(Modifier.PRIVATE)) {
        logMessage("Skipping private field: " + field);
        continue;
      }
      if (field.getModifiers().contains(Modifier.STATIC)) {
        logMessage("Skipping non-static field: " + field);
        continue;
      }

      String fieldName = field.getSimpleName().toString();
      if (fieldName.endsWith("_PREFIX")) {
        logMessage("Skipping non-config prefix constant field: " + field);
        continue;
      }
      if (fieldName.equals("TEZ_SITE_XML")) {
        logMessage("Skipping constant field: " + field);
        continue;
      }

      if (fieldName.endsWith("_DEFAULT")) {

        String name = fieldName.substring(0,
                fieldName.lastIndexOf("_DEFAULT"));
        if (!configProperties.containsKey(name)) {
          configProperties.put(name, new ConfigProperty());
        }
        ConfigProperty configProperty = configProperties.get(name);
        if (field.getConstantValue() == null) {
          String val = field.getConstantValue().toString();
          logMessage("Got null constant value"
                  + ", name=" + name
                  + ", field=" + fieldName
                  + ", val=" + val);
          configProperty.defaultValue = val;
        } else {
          configProperty.defaultValue = field.getConstantValue().toString();
        }
        configProperty.inferredType = field.getSimpleName().toString();

        if (name.equals("TEZ_AM_STAGING_DIR") && configProperty.defaultValue != null) {
          String defaultValue = configProperty.defaultValue;
          defaultValue = defaultValue.replace(System.getProperty("user.name"), "${user.name}");
          configProperty.defaultValue = defaultValue;
        }

        continue;
      }

      if (!configProperties.containsKey(fieldName)) {
        configProperties.put(fieldName, new ConfigProperty());
      }
      ConfigProperty configProperty = configProperties.get(fieldName);
      configProperty.propertyName = field.getConstantValue().toString();

      List<? extends AnnotationMirror> annotationDescs = field.getAnnotationMirrors();

      for (AnnotationMirror annotationDesc : annotationDescs) {
        String elementFqName = annotationDesc.getAnnotationType().asElement().toString();
        if (elementFqName.equals(
                Private.class.getCanonicalName())) {
          configProperty.isPrivate = true;
        }
        if (elementFqName.equals(
                Unstable.class.getCanonicalName())) {
          configProperty.isUnstable = true;
        }
        if (elementFqName.equals(
                Evolving.class.getCanonicalName())) {
          configProperty.isEvolving = true;
        }
        if (elementFqName.equals(
                ConfigurationProperty.class.getCanonicalName())) {
          configProperty.isValidConfigProp = true;

          for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> element
              : annotationDesc.getElementValues().entrySet()) {
            if (element.getKey().getSimpleName().toString().equals("type")) {
              configProperty.type = stripQuotes(element.getValue().getValue().toString());
            } else {
              logMessage("Unhandled annotation property: " + element.getKey().getSimpleName());
            }
          }
        }
      }

      configProperty.description = docTrees.getDocCommentTree(field).getFullBody().toString();
    }
  }

  private static String stripQuotes(String s) {
    if (s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"') {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }

  @Override
  public Set<? extends Option> getSupportedOptions() {
    Option[] options = {
        new Option() {
            private final List<String> someOption = Arrays.asList(
                    "-debug",
                    "--debug"
            );

            @Override
            public int getArgumentCount() {
              return 0;
            }

            @Override
            public String getDescription() {
              return "Debug mode";
            }

            @Override
            public Option.Kind getKind() {
              return Kind.STANDARD;
            }

            @Override
            public List<String> getNames() {
              return someOption;
            }

            @Override
            public String getParameters() {
              return "";
            }

            @Override
            public boolean process(String opt, List<String> arguments) {
              debugMode = true;
              return true;
            }
          }
    };
    return new HashSet<>(Arrays.asList(options));
  }
}
