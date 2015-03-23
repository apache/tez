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

package org.apache.tez.mapreduce.examples.processor;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.input.UnorderedKVInput;


public class FilterByWordOutputProcessor extends SimpleMRProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(MapProcessor.class);

  public FilterByWordOutputProcessor(ProcessorContext context) {
    super(context);
  }


  @Override
  public void handleEvents(List<Event> processorEvents) {
    throw new UnsupportedOperationException("Not expecting any events to the broadcast output processor");
  }

  @Override
  public void close() throws Exception {
    LOG.info("Broadcast Output Processor closing. Nothing to do");
  }

  @Override
  public void run() throws Exception {
    
    if (inputs.size() != 1) {
      throw new IllegalStateException("FilterByWordOutputProcessor processor can only work with a single input");
    }

    if (outputs.size() != 1) {
      throw new IllegalStateException("FilterByWordOutputProcessor processor can only work with a single output");
    }

    for (LogicalInput input : inputs.values()) {
      input.start();
    }
    for (LogicalOutput output : outputs.values()) {
      output.start();
    }

    LogicalInput li = inputs.values().iterator().next();
    if (! (li instanceof UnorderedKVInput)) {
      throw new IllegalStateException("FilterByWordOutputProcessor processor can only work with ShuffledUnorderedKVInput");
    }

    LogicalOutput lo = outputs.values().iterator().next();
    if (! (lo instanceof MROutput)) {
      throw new IllegalStateException("FilterByWordOutputProcessor processor can only work with MROutput");
    }

    UnorderedKVInput kvInput = (UnorderedKVInput) li;
    MROutput mrOutput = (MROutput) lo;

    KeyValueReader kvReader = kvInput.getReader();
    KeyValueWriter kvWriter = mrOutput.getWriter();
    while (kvReader.next()) {
      Object key = kvReader.getCurrentKey();
      Object value = kvReader.getCurrentValue();

      kvWriter.write(key, value);
    }
  }
}
