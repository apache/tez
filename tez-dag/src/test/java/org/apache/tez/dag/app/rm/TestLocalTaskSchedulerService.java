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

package org.apache.tez.dag.app.rm;

import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestLocalTaskSchedulerService {

  LocalTaskSchedulerService ltss ;
  int core =10;

  @Test
  public void testCreateResource() {
    Resource resource;
    //value in integer
    long value = 4*1024*1024;
    resource = ltss.createResource(value,core);
    Assert.assertEquals((int)(value/(1024*1024)),resource.getMemory());
  }

  @Test
  public void testCreateResourceLargerThanIntMax() {
    //value beyond integer but within Long.MAX_VALUE
    try {
      ltss.createResource(Long.MAX_VALUE, core);
      fail("No exception thrown.");
    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalArgumentException);
      assertTrue(ex.getMessage().contains("Out of range:"));
    }
  }

  @Test
  public void testCreateResourceWithNegativeValue() {
    //value is Long.MAX_VALUE*1024*1024,
    // it will be negative after it is passed to createResource

    try {
      ltss.createResource((Long.MAX_VALUE*1024*1024), core);
      fail("No exception thrown.");
    } catch (Exception ex) {
      assertTrue(ex instanceof IllegalArgumentException);
      assertTrue(ex.getMessage().contains("Negative Memory or Core provided!"));
    }
  }
}
