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

package org.apache.tez.hadoop.shim;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.hadoop.shim.DummyShimProvider.DummyShim;
import org.junit.Assert;
import org.junit.Test;

public class TestHadoopShimsLoader {

  @Test
  public void testBasicLoader() {
    HadoopShimsLoader loader = new HadoopShimsLoader(new Configuration(false));
    HadoopShim shim = loader.getHadoopShim();
    Assert.assertNotNull(shim);
    Assert.assertEquals(DefaultHadoopShim.class, shim.getClass());
  }

  @Test
  public void testLoaderOverride() {
    Configuration conf = new Configuration(false);
    conf.set(HadoopShimsLoader.TEZ_HADOOP_SHIM_PROVIDER_CLASS, DummyShimProvider.class.getName());
    HadoopShimsLoader loader = new HadoopShimsLoader(conf, true);
    HadoopShim shim = loader.getHadoopShim();
    Assert.assertNotNull(shim);
    Assert.assertEquals(DummyShim.class, shim.getClass());
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidOverride() {
    Configuration conf = new Configuration(false);
    conf.set(HadoopShimsLoader.TEZ_HADOOP_SHIM_PROVIDER_CLASS, "org.apache.tez.foo");
    HadoopShimsLoader loader = new HadoopShimsLoader(conf, true);
    HadoopShim shim = loader.getHadoopShim();
  }

}
