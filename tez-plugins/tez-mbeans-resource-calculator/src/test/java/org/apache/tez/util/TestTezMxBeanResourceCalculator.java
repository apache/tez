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

package org.apache.tez.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestTezMxBeanResourceCalculator {

  private ResourceCalculatorProcessTree resourceCalculator;

  @Before
  public void setup() throws Exception {
    Configuration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_TASK_RESOURCE_CALCULATOR_PROCESS_TREE_CLASS,
        TezMxBeanResourceCalculator.class.getName());

    Class<? extends ResourceCalculatorProcessTree> clazz = conf.getClass(
        TezConfiguration.TEZ_TASK_RESOURCE_CALCULATOR_PROCESS_TREE_CLASS, null,
        ResourceCalculatorProcessTree.class);
    resourceCalculator = ResourceCalculatorProcessTree.getResourceCalculatorProcessTree(
        System.getenv().get("JVM_PID"), clazz, conf);
  }

  @After
  public void teardown() {
    resourceCalculator = null;
  }

  @Test(timeout=5000)
  public void testResourceCalculator() {
    Assert.assertTrue(resourceCalculator instanceof TezMxBeanResourceCalculator);
    Assert.assertTrue(resourceCalculator.getCumulativeCpuTime() > 0);
    Assert.assertTrue(resourceCalculator.getCumulativeVmem() > 0);
    Assert.assertTrue(resourceCalculator.getCumulativeRssmem() > 0);
    Assert.assertTrue(resourceCalculator.getProcessTreeDump().equals(""));
    Assert.assertTrue(resourceCalculator.checkPidPgrpidForMatch());
  }

}
