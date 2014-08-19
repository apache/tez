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

package org.apache.tez.mapreduce.output;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.tez.runtime.api.OutputContext;

@Private
public class MROutputLegacy extends MROutput {

  /**
   * Create an {@link org.apache.tez.mapreduce.output.MROutput.MROutputConfigBuilder}
   *
   * @param conf         Configuration for the {@link MROutput}
   * @param outputFormat OutputFormat derived class
   * @return {@link org.apache.tez.mapreduce.output.MROutput.MROutputConfigBuilder}
   */
  public static MROutputConfigBuilder createConfigBuilder(Configuration conf, Class<?> outputFormat) {
    return MROutput.createConfigBuilder(conf, outputFormat)
        .setOutputClassName(MROutputLegacy.class.getName());
  }

  /**
   * Create an {@link org.apache.tez.mapreduce.output.MROutput.MROutputConfigBuilder} for a FileOutputFormat
   *
   * @param conf         Configuration for the {@link MROutput}
   * @param outputFormat FileInputFormat derived class
   * @param outputPath   Output path
   * @return {@link org.apache.tez.mapreduce.output.MROutput.MROutputConfigBuilder}
   */
  public static MROutputConfigBuilder createConfigBuilder(Configuration conf, Class<?> outputFormat,
                                                          String outputPath) {
    return MROutput.createConfigBuilder(conf, outputFormat, outputPath)
        .setOutputClassName(MROutputLegacy.class.getName());
  }

  public MROutputLegacy(OutputContext outputContext, int numPhysicalOutputs) {
    super(outputContext, numPhysicalOutputs);
  }

  public OutputCommitter getOutputCommitter() {
    return committer;
  }
}
