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
import org.junit.Assert;
import org.junit.Test;

public class TestHadoop25_26_27ShimProvider {

  @Test
  public void testShimProvider() {
    HadoopShim25_26_27Provider provider = new HadoopShim25_26_27Provider();
    Assert.assertNull(provider.createHadoopShim("foo", 2, 2));
    Assert.assertNull(provider.createHadoopShim("foo", 2, 1));
    Assert.assertNull(provider.createHadoopShim("foo", 2, 3));
    Assert.assertNull(provider.createHadoopShim("foo", 2, 8));
    Assert.assertNull(provider.createHadoopShim("foo", 3, 3));
    Assert.assertNotNull(provider.createHadoopShim("foo", 2, 5));
    Assert.assertNotNull(provider.createHadoopShim("foo", 2, 6));
    Assert.assertNotNull(provider.createHadoopShim("foo", 2, 7));

    Assert.assertEquals(HadoopShim27.class,
        provider.createHadoopShim("foo", 2, 7).getClass());
  }

  @Test
  public void testLoaderOverride() {
    Configuration conf = new Configuration(false);
    // Set shim and version to ensure that hadoop version from the build does not create
    // a mismatch
    conf.set(HadoopShimsLoader.TEZ_HADOOP_SHIM_PROVIDER_CLASS,
        HadoopShim25_26_27Provider.class.getName());
    conf.set(HadoopShimsLoader.TEZ_HADOOP_SHIM_HADOOP_VERSION_OVERRIDE, "2.7.0");
    HadoopShimsLoader loader = new HadoopShimsLoader(conf, true);
    HadoopShim shim = loader.getHadoopShim();
    Assert.assertNotNull(shim);
    Assert.assertEquals(HadoopShim27.class, shim.getClass());
  }

  @Test
  public void testInvalidVersion() {
    Configuration conf = new Configuration(false);
    // Set incompatible version so that shim in this module does not match
    conf.set(HadoopShimsLoader.TEZ_HADOOP_SHIM_HADOOP_VERSION_OVERRIDE, "2.9.0");
    HadoopShimsLoader loader = new HadoopShimsLoader(conf, true);
    HadoopShim shim = loader.getHadoopShim();
    Assert.assertNotNull(shim);
    Assert.assertEquals(DefaultHadoopShim.class, shim.getClass());
  }

  @Test
  public void testLoaderOverrideInvalidVersion() {
    Configuration conf = new Configuration(false);
    // Set incompatible version so that override shim does not return a valid shim
    conf.set(HadoopShimsLoader.TEZ_HADOOP_SHIM_PROVIDER_CLASS,
        HadoopShim25_26_27Provider.class.getName());
    conf.set(HadoopShimsLoader.TEZ_HADOOP_SHIM_HADOOP_VERSION_OVERRIDE, "2.1.0");
    HadoopShimsLoader loader = new HadoopShimsLoader(conf, true);
    HadoopShim shim = loader.getHadoopShim();
    Assert.assertNotNull(shim);
    Assert.assertEquals(DefaultHadoopShim.class, shim.getClass());
  }

}
