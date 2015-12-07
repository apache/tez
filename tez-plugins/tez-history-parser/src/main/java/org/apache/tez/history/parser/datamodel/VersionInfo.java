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

package org.apache.tez.history.parser.datamodel;

public class VersionInfo {

  private final String buildTime;
  private final String revision;
  private final String version;

  public VersionInfo(String buildTime, String revision, String version) {
    this.buildTime = buildTime;
    this.revision = revision;
    this.version = version;
  }

  public String getBuildTime() {
    return buildTime;
  }

  public String getRevision() {
    return revision;
  }

  public String getVersion() {
    return version;
  }

}
