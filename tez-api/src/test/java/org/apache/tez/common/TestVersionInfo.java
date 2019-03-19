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

import org.junit.Assert;
import org.junit.Test;

public class TestVersionInfo {

  private static final String VERSION = "0.6.0-SNAPSHOT";
  private static final String REVISION = "d523db65804a5742ce50824e6fcfb8a04d184c0d";
  private static final String BUILD_TIME = "20141024-1052";
  private static final String SCM_URL = "scm:git:https://gitbox.apache.org/repos/asf/tez.git";

  @Test(timeout = 5000)
  public void testTest1File() {
    VersionInfo versionInfo = new VersionInfo("test1");
    Assert.assertEquals(VERSION, versionInfo.getVersion());
    Assert.assertEquals(REVISION, versionInfo.getRevision());
    Assert.assertEquals(BUILD_TIME, versionInfo.getBuildTime());
    Assert.assertEquals(SCM_URL, versionInfo.getSCMURL());
  }

  @Test(timeout = 5000)
  public void testTest2File() {
    VersionInfo versionInfo = new VersionInfo("test2");
    Assert.assertEquals(VERSION, versionInfo.getVersion());
    Assert.assertEquals(REVISION, versionInfo.getRevision());
    Assert.assertEquals(BUILD_TIME, versionInfo.getBuildTime());
    Assert.assertEquals(VersionInfo.UNKNOWN, versionInfo.getSCMURL());
  }

  @Test(timeout = 5000)
  public void testTest3File() {
    VersionInfo versionInfo = new VersionInfo("test3");
    Assert.assertEquals(VERSION, versionInfo.getVersion());
    Assert.assertEquals(REVISION, versionInfo.getRevision());
    Assert.assertEquals("", versionInfo.getBuildTime());
    Assert.assertEquals(SCM_URL, versionInfo.getSCMURL());
  }

  @Test(timeout = 5000)
  public void testNonExistentFile() {
    VersionInfo versionInfo = new VersionInfo("test4");
  }

}
