/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.mapreduce;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;

public class TokenProcessor extends SimpleProcessor {
  private static final String INPUT = "Input";
  private static final String SUMMATION = "Summation";

  private final IntWritable one = new IntWritable(1);
  private final Text word = new Text();

  public TokenProcessor(ProcessorContext context) {
    super(context);
  }

  @Override
  public void run() throws Exception {
    Preconditions.checkArgument(getInputs().size() == 1);
    Preconditions.checkArgument(getOutputs().size() == 1);
    // the recommended approach is to cast the reader/writer to a specific type instead
    // of casting the input/output. This allows the actual input/output type to be replaced
    // without affecting the semantic guarantees of the data type that are represented by
    // the reader and writer.
    // The inputs/outputs are referenced via the names assigned in the DAG.
    KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT).getReader();
    KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(SUMMATION).getWriter();
    while (kvReader.next()) {
      StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        // Count 1 every time a word is observed. Word is the key a 1 is the value
        kvWriter.write(word, one);
      }
    }
  }
}
