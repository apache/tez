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

import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.runtime.library.common.Constants;
import org.junit.Test;

public class TestDeprecatedKeys {

  @Test
  public void verifyReduceKeyTranslation() {
    JobConf jobConf = new JobConf();

    jobConf.setFloat(MRJobConfig.SHUFFLE_INPUT_BUFFER_PERCENT, 0.4f);
    jobConf.setLong(MRJobConfig.REDUCE_MEMORY_TOTAL_BYTES, 20000l);
    jobConf.setInt(MRJobConfig.IO_SORT_FACTOR, 2000);
    jobConf.setFloat(MRJobConfig.SHUFFLE_MEMORY_LIMIT_PERCENT, 0.55f);
    jobConf.setFloat(MRJobConfig.REDUCE_MEMTOMEM_THRESHOLD, 0.60f);
    jobConf.setFloat(MRJobConfig.SHUFFLE_MERGE_PERCENT, 0.22f);
    jobConf.setBoolean(MRJobConfig.REDUCE_MEMTOMEM_ENABLED, true);
    jobConf.setFloat(MRJobConfig.REDUCE_INPUT_BUFFER_PERCENT, 0.33f);

    MultiStageMRConfToTezTranslator.translateVertexConfToTez(jobConf, null);

    assertEquals(0.4f, jobConf.getFloat(
        TezJobConfig.TEZ_RUNTIME_SHUFFLE_INPUT_BUFFER_PERCENT, 0f), 0.01f);
    assertEquals(20000l, jobConf.getLong(Constants.TEZ_RUNTIME_TASK_MEMORY, 0));
    assertEquals(2000,
        jobConf.getInt(TezJobConfig.TEZ_RUNTIME_IO_SORT_FACTOR, 0));
    assertEquals(0.55f, jobConf.getFloat(
        TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMORY_LIMIT_PERCENT, 0), 0.01f);
    assertEquals(0.60f,
        jobConf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MEMTOMEM_SEGMENTS, 0),
        0.01f);
    assertEquals(0.22f,
        jobConf.getFloat(TezJobConfig.TEZ_RUNTIME_SHUFFLE_MERGE_PERCENT, 0),
        0.01f);
    assertEquals(true, jobConf.getBoolean(
        TezJobConfig.TEZ_RUNTIME_SHUFFLE_ENABLE_MEMTOMEM, false));
    assertEquals(0.33f,
        jobConf.getFloat(TezJobConfig.TEZ_RUNTIME_INPUT_BUFFER_PERCENT, 0),
        0.01f);
  }

}
