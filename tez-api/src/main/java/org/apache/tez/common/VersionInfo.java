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

package org.apache.tez.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ClassUtil;

import com.google.common.annotations.VisibleForTesting;

@Private
@Unstable
public class VersionInfo {
  private static final Logger LOG = LoggerFactory.getLogger(VersionInfo.class);

  private final Properties info;
  private final String component;

  private static final String VERSION = "version";
  private static final String REVISION = "revision";
  private static final String BUILD_TIME = "buildtime";
  private static final String SCM_URL = "scmurl";

  public static final String UNKNOWN = "Unknown";

  protected VersionInfo(String component) {
    this.component = component;
    info = new Properties();
    String versionInfoFile = "/" + component + "-version-info.properties";
    InputStream is = null;
    try {
      is = this.getClass().getResourceAsStream(versionInfoFile);
      if (is == null) {
        throw new IOException("Resource not found: " + versionInfoFile);
      }
      info.load(is);
    } catch (IOException ex) {
      LOG.warn("Could not read '" + versionInfoFile + "', " + ex.toString(), ex);
    } finally {
      IOUtils.closeStream(is);
    }
  }

  @VisibleForTesting
  @Private
  protected VersionInfo(String component, String version, String revision,
      String buildTime, String scmUrl) {
    this.info = new Properties();
    this.component = component;
    info.setProperty(VERSION, version);
    info.setProperty(REVISION, revision);
    info.setProperty(BUILD_TIME, buildTime);
    info.setProperty(SCM_URL, scmUrl);
  }

  public String getVersion() {
    return info.getProperty(VERSION, UNKNOWN);
  }

  public String getBuildTime() {
    return info.getProperty(BUILD_TIME, UNKNOWN);
  }

  public String getRevision() {
    return info.getProperty(REVISION, UNKNOWN);
  }

  public String getSCMURL() {
    return info.getProperty(SCM_URL, UNKNOWN);
  }

  @Override
  public String toString() {
    return "[ component=" + component
        + ", version=" + getVersion()
        + ", revision=" + getRevision()
        + ", SCM-URL=" + getSCMURL()
        + ", buildTime=" + getBuildTime()
        + " ]";
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Invalid no. of args. Usage: VersionInfo <component-name>");
      System.exit(-1);
    }

    VersionInfo versionInfo = new VersionInfo(args[0]);
    System.out.println("VersionInfo: " + versionInfo.toString());
    System.out.println("This command was run using " +
        ClassUtil.findContainingJar(VersionInfo.class));
  }

}
