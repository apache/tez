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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.runtime.library.common.ConfigUtils;
import org.junit.Test;

public class TestConfigTranslationMRToTez {

  @Test
  // Tests derived keys - i.e. the actual key is not set, but the value is 
  // derived from a fallback key.
  public void testComplexKeys() {

    JobConf confVertex1 = new JobConf();
    JobConf confVertex2 = new JobConf();
    
    confVertex1.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS, IntWritable.class.getName());
    confVertex2.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS, ByteWritable.class.getName());
    
    confVertex1.unset(MRJobConfig.KEY_COMPARATOR);
    confVertex1.unset(MRJobConfig.GROUP_COMPARATOR_CLASS);
    confVertex2.unset(MRJobConfig.KEY_COMPARATOR);
    confVertex2.unset(MRJobConfig.GROUP_COMPARATOR_CLASS);
    
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(confVertex1, null);
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(confVertex2,
        confVertex1);
    
    assertEquals(IntWritable.Comparator.class.getName(), ConfigUtils
        .getIntermediateOutputKeyComparator(confVertex1).getClass().getName());
    assertEquals(IntWritable.Comparator.class.getName(), ConfigUtils
        .getIntermediateInputKeyComparator(confVertex2).getClass().getName());
  }

  @Test
  public void testMultiStageConversion() {
    JobConf confVertex1 = new JobConf();
    confVertex1.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        IntWritable.class.getName());
    confVertex1.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        IntWritable.class.getName());
    confVertex1.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);

    JobConf confVertex2 = new JobConf();
    confVertex2.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        LongWritable.class.getName());
    confVertex2.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        LongWritable.class.getName());
    confVertex2.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, false);

    JobConf confVertex3 = new JobConf();
    confVertex3.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS,
        ByteWritable.class.getName());
    confVertex3.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS,
        ByteWritable.class.getName());
    confVertex3.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);

    JobConf confVertex4 = new JobConf();
    confVertex4.set(MRJobConfig.MAP_OUTPUT_KEY_CLASS, Text.class.getName());
    confVertex4.set(MRJobConfig.MAP_OUTPUT_VALUE_CLASS, Text.class.getName());

    MultiStageMRConfToTezTranslator.translateVertexConfToTez(confVertex1, null);
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(confVertex2,
        confVertex1);
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(confVertex3,
        confVertex2);
    MultiStageMRConfToTezTranslator.translateVertexConfToTez(confVertex4,
        confVertex3);

    // Verify input params for first vertex.
    assertFalse(ConfigUtils.isIntermediateInputCompressed(confVertex1));
    assertNull(ConfigUtils.getIntermediateInputKeyClass(confVertex1));
    assertNull(ConfigUtils.getIntermediateInputValueClass(confVertex1));

    // Verify edge between v1 and v2
    assertEquals(IntWritable.class.getName(), ConfigUtils
        .getIntermediateOutputKeyClass(confVertex1).getName());
    assertEquals(IntWritable.class.getName(), ConfigUtils
        .getIntermediateOutputValueClass(confVertex1).getName());
    assertEquals(IntWritable.class.getName(), ConfigUtils
        .getIntermediateInputKeyClass(confVertex2).getName());
    assertEquals(IntWritable.class.getName(), ConfigUtils
        .getIntermediateInputValueClass(confVertex2).getName());
    assertTrue(ConfigUtils.shouldCompressIntermediateOutput(confVertex1));
    assertTrue(ConfigUtils.isIntermediateInputCompressed(confVertex2));

    // Verify edge between v2 and v3
    assertEquals(LongWritable.class.getName(), ConfigUtils
        .getIntermediateOutputKeyClass(confVertex2).getName());
    assertEquals(LongWritable.class.getName(), ConfigUtils
        .getIntermediateOutputValueClass(confVertex2).getName());
    assertEquals(LongWritable.class.getName(), ConfigUtils
        .getIntermediateInputKeyClass(confVertex3).getName());
    assertEquals(LongWritable.class.getName(), ConfigUtils
        .getIntermediateInputValueClass(confVertex3).getName());
    assertFalse(ConfigUtils.shouldCompressIntermediateOutput(confVertex2));
    assertFalse(ConfigUtils.isIntermediateInputCompressed(confVertex3));

    // Verify edge between v3 and v4
    assertEquals(ByteWritable.class.getName(), ConfigUtils
        .getIntermediateOutputKeyClass(confVertex3).getName());
    assertEquals(ByteWritable.class.getName(), ConfigUtils
        .getIntermediateOutputValueClass(confVertex3).getName());
    assertEquals(ByteWritable.class.getName(), ConfigUtils
        .getIntermediateInputKeyClass(confVertex4).getName());
    assertEquals(ByteWritable.class.getName(), ConfigUtils
        .getIntermediateInputValueClass(confVertex4).getName());
    assertTrue(ConfigUtils.shouldCompressIntermediateOutput(confVertex3));
    assertTrue(ConfigUtils.isIntermediateInputCompressed(confVertex4));

    // Verify output params for first vertex.
    assertFalse(ConfigUtils.shouldCompressIntermediateOutput(confVertex4));
    
    // Verify Edge configuration
    Configuration edge1OutputConf = MultiStageMRConfToTezTranslator
        .getOutputConfOnEdge(confVertex1, confVertex2);
    Configuration edge1InputConf = MultiStageMRConfToTezTranslator
        .getInputConfOnEdge(confVertex1, confVertex2);
    
    assertEquals(IntWritable.class.getName(), ConfigUtils
        .getIntermediateOutputKeyClass(edge1OutputConf).getName());
    assertEquals(IntWritable.class.getName(), ConfigUtils
        .getIntermediateOutputValueClass(edge1OutputConf).getName());
    assertTrue(ConfigUtils.shouldCompressIntermediateOutput(edge1OutputConf));
    
    assertEquals(IntWritable.class.getName(), ConfigUtils
        .getIntermediateInputKeyClass(edge1InputConf).getName());
    assertEquals(IntWritable.class.getName(), ConfigUtils
        .getIntermediateInputValueClass(edge1InputConf).getName());
    assertTrue(ConfigUtils.isIntermediateInputCompressed(edge1InputConf));
    
  }
}
