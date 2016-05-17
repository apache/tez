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

package org.apache.tez.common;

import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.TezUncheckedException;
import org.junit.Assert;
import org.junit.Test;

public class TestJavaOptsChecker {

  private final JavaOptsChecker javaOptsChecker = new JavaOptsChecker();

  @Test(timeout = 5000)
  public void testBasicChecker() throws TezException {
    javaOptsChecker.checkOpts(TezConfiguration.TEZ_TASK_LAUNCH_CMD_OPTS_DEFAULT);
  }

  @Test(timeout = 5000)
  public void testMultipleGC() {
    // Clashing GC values
    String opts = "-XX:+UseConcMarkSweepGC -XX:+UseG1GC -XX:+UseParallelGC ";
    try {
      javaOptsChecker.checkOpts(opts);
      Assert.fail("Expected check to fail with opts=" + opts);
    } catch (TezException e) {
      Assert.assertTrue(e.getMessage(),
          e.getMessage().contains("Invalid/conflicting GC options found"));
    }
  }

  @Test(timeout = 5000)
  public void testPositiveNegativeOpts() throws TezException {
    // Multiple positive GC values
    String opts = "-XX:+UseConcMarkSweepGC -XX:+UseG1GC -XX:+UseParallelGC -XX:-UseG1GC ";
    try {
      javaOptsChecker.checkOpts(opts);
      Assert.fail("Expected check to fail with opts=" + opts);
    } catch (TezException e) {
      Assert.assertTrue(e.getMessage(),
          e.getMessage().contains("Invalid/conflicting GC options found"));
    }

    // Positive following a negative is still a positive
    opts = " -XX:-UseG1GC -XX:+UseParallelGC -XX:-UseG1GC  -XX:+UseG1GC";
    try {
      javaOptsChecker.checkOpts(opts);
      Assert.fail("Expected check to fail with opts=" + opts);
    } catch (TezException e) {
      Assert.assertTrue(e.getMessage(),
          e.getMessage().contains("Invalid/conflicting GC options found"));
    }

    // Order of positive and negative matters
    opts = " -XX:+UseG1GC -XX:-UseG1GC -XX:+UseParallelGC -XX:-UseG1GC  -XX:+UseG1GC";
    try {
      javaOptsChecker.checkOpts(opts);
      Assert.fail("Expected check to fail with opts=" + opts);
    } catch (TezException e) {
      Assert.assertTrue(e.getMessage(),
          e.getMessage().contains("Invalid/conflicting GC options found"));
    }

    // Sanity check for good condition
    opts = " -XX:+UseG1GC -XX:+UseParallelGC -XX:-UseG1GC ";
    javaOptsChecker.checkOpts(opts);

    // Invalid negative can be ignored
    opts = " -XX:+UseG1GC -XX:+UseParallelGC -XX:-UseG1GC -XX:-UseConcMarkSweepGC ";
    javaOptsChecker.checkOpts(opts);

  }

  @Test(timeout = 5000)
  public void testSpecialCaseNonConflictingGCOptions() throws TezException {
    String opts = " -XX:+UseParNewGC -XX:+UseConcMarkSweepGC ";
    javaOptsChecker.checkOpts(opts);

    opts += " -XX:+DisableExplicitGC ";
    javaOptsChecker.checkOpts(opts);

    opts += " -XX:-UseG1GC ";
    javaOptsChecker.checkOpts(opts);

    opts += " -XX:+UseG1GC ";
    try {
      javaOptsChecker.checkOpts(opts);
      Assert.fail("Expected check to fail with opts=" + opts);
    } catch (TezException e) {
      Assert.assertTrue(e.getMessage(),
          e.getMessage().contains("Invalid/conflicting GC options found"));
    }


  }

}
