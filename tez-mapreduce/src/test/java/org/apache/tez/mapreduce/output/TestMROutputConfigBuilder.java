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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestMROutputConfigBuilder {

  @Test(timeout = 5000)
  public void testNewAPI() {
    Configuration conf = new Configuration();
    try {
      MROutput.createConfigBuilder(conf, TextOutputFormat.class).build();
      fail();
    } catch (TezUncheckedException e) {
      assertEquals("OutputPaths must be specified for OutputFormats based "
          + "on org.apache.hadoop.mapreduce.lib.output.FileOutputFormat "
          +"or org.apache.hadoop.mapred.FileOutputFormat", e.getMessage());
    }
    MROutput.createConfigBuilder(conf, TextOutputFormat.class, "/tmp/output").build();

    // no outputPaths needs to be specified
    MROutput.createConfigBuilder(conf, DBOutputFormat.class).build();
  }

  @Test(timeout = 5000)
  public void testNewAPI_ThroughConf() {
    Configuration conf = new Configuration();
    try {
      MROutput.createConfigBuilder(conf, null).build();
      fail();
    } catch (TezUncheckedException e) {
      assertEquals("no outputFormat setting on Configuration, useNewAPI:true",  e.getMessage());
    }

    // wrong output_format class
    conf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
        org.apache.hadoop.mapred.TextOutputFormat.class.getName());
    try {
      MROutput.createConfigBuilder(conf, null).build();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("outputFormat must be assignable from org.apache.hadoop.mapreduce.OutputFormat",
          e.getMessage());
    }

    // correct output_format class, but no output_dir specified
    conf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,
        TextOutputFormat.class.getName());
    try {
      MROutput.createConfigBuilder(conf, null).build();
      fail();
    } catch (TezUncheckedException e) {
      assertEquals("OutputPaths must be specified for OutputFormats based "
          + "on org.apache.hadoop.mapreduce.lib.output.FileOutputFormat "
          +"or org.apache.hadoop.mapred.FileOutputFormat", e.getMessage());
    }

    conf.set(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR, "/tmp/output");
    MROutput.createConfigBuilder(conf, null).build();
  }

  @Test(timeout = 5000 )
  public void testOldAPI() {
    Configuration conf = new Configuration();
    try {
      MROutput.createConfigBuilder(conf, org.apache.hadoop.mapred.TextOutputFormat.class).build();
      fail();
    } catch (TezUncheckedException e) {
      assertEquals("OutputPaths must be specified for OutputFormats based "
          + "on org.apache.hadoop.mapreduce.lib.output.FileOutputFormat "
          +"or org.apache.hadoop.mapred.FileOutputFormat", e.getMessage());
    }
    MROutput.createConfigBuilder(conf, org.apache.hadoop.mapred.TextOutputFormat.class,
        "/tmp/output").build();

    // no outputPaths needs to be specified
    MROutput.createConfigBuilder(conf, org.apache.hadoop.mapred.lib.db.DBOutputFormat.class).build();
  }

  @Test(timeout = 5000)
  public void testOldAPI_ThroughConf() {
    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.NEW_API_REDUCER_CONFIG, false);
    try {
      MROutput.createConfigBuilder(conf, null).build();
      fail();
    } catch (TezUncheckedException e) {
      assertEquals("no outputFormat setting on Configuration, useNewAPI:false",  e.getMessage());
    }

    // wrong output_format class
    conf.set("mapred.output.format.class",
        TextOutputFormat.class.getName());
    try {
      MROutput.createConfigBuilder(conf, null).build();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("outputFormat must be assignable from org.apache.hadoop.mapred.OutputFormat",
          e.getMessage());
    }

    // correct output_format class, but no output_dir specified
    conf.set("mapred.output.format.class",
        org.apache.hadoop.mapred.TextOutputFormat.class.getName());
    try {
      MROutput.createConfigBuilder(conf, null).build();
      fail();
    } catch (TezUncheckedException e) {
      assertEquals("OutputPaths must be specified for OutputFormats based "
          + "on org.apache.hadoop.mapreduce.lib.output.FileOutputFormat "
          +"or org.apache.hadoop.mapred.FileOutputFormat", e.getMessage());
    }

    conf.set(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR, "/tmp/output");
    MROutput.createConfigBuilder(conf, null).build();
  }
}
