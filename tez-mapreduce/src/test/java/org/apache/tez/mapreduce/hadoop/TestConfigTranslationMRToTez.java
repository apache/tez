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

package org.apache.tez.mapreduce.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.junit.Test;

public class TestConfigTranslationMRToTez {

  @Test
  // Tests derived keys - i.e. the actual key is not set, but the value is 
  // derived from a fallback key.
  public void testComplexKeys() {

    JobConf confVertex1 = new JobConf();
    
    confVertex1.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS, IntWritable.class.getName());
    
    confVertex1.unset(MRJobConfig.KEY_COMPARATOR);
    confVertex1.unset(MRJobConfig.GROUP_COMPARATOR_CLASS);
    
    MRHelpers.translateMRConfToTez(confVertex1);

    assertEquals(IntWritable.Comparator.class.getName(), ConfigUtils
        .getIntermediateOutputKeyComparator(confVertex1).getClass().getName());
    assertEquals(IntWritable.Comparator.class.getName(), ConfigUtils
        .getIntermediateInputKeyComparator(confVertex1).getClass().getName());
  }

  @Test
  public void testMRToTezKeyTranslation() {
    JobConf confVertex1 = new JobConf();
    confVertex1.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        IntWritable.class.getName());
    confVertex1.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        LongWritable.class.getName());
    confVertex1.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);

    MRHelpers.translateMRConfToTez(confVertex1);

    // Verify translation
    assertEquals(IntWritable.class.getName(), ConfigUtils
        .getIntermediateOutputKeyClass(confVertex1).getName());
    assertEquals(LongWritable.class.getName(), ConfigUtils
        .getIntermediateOutputValueClass(confVertex1).getName());
    assertEquals(IntWritable.class.getName(), ConfigUtils
        .getIntermediateInputKeyClass(confVertex1).getName());
    assertEquals(LongWritable.class.getName(), ConfigUtils
        .getIntermediateInputValueClass(confVertex1).getName());
    assertTrue(ConfigUtils.shouldCompressIntermediateOutput(confVertex1));
    assertTrue(ConfigUtils.isIntermediateInputCompressed(confVertex1));
  }
}
