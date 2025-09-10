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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;

import com.google.common.base.Preconditions;

public class SumProcessor extends SimpleMRProcessor {
  public SumProcessor(ProcessorContext context) {
    super(context);
  }

  private static final String OUTPUT = "Output";
  private static final String TOKENIZER = "Tokenizer";

  @Override
  public void run() throws Exception {
    Preconditions.checkArgument(getInputs().size() == 1);
    Preconditions.checkArgument(getOutputs().size() == 1);
    KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
    // The KeyValues reader provides all values for a given key. The aggregation of values per key
    // is done by the LogicalInput. Since the key is the word and the values are its counts in
    // the different TokenProcessors, summing all values per key provides the sum for that word.
    KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(TOKENIZER).getReader();
    while (kvReader.next()) {
      Text word = (Text) kvReader.getCurrentKey();
      int sum = 0;
      for (Object value : kvReader.getCurrentValues()) {
        sum += ((IntWritable) value).get();
      }
      kvWriter.write(word, new IntWritable(sum));
    }
    // deriving from SimpleMRProcessor takes care of committing the output
    // It automatically invokes the commit logic for the OutputFormat if necessary.
  }
}
