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

package org.apache.tez.mapreduce.examples.processor;

import java.util.List;
import java.util.Map;

import org.apache.tez.common.ProgressHelper;
import org.apache.tez.runtime.api.TaskFailureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.common.TezUtils;
import org.apache.tez.mapreduce.examples.FilterLinesByWord;
import org.apache.tez.mapreduce.examples.FilterLinesByWord.TextLongPair;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.output.UnorderedKVOutput;

public class FilterByWordInputProcessor extends AbstractLogicalIOProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FilterByWordInputProcessor.class);

  private String filterWord;
  protected Map<String, LogicalInput> inputs;
  protected Map<String, LogicalOutput> outputs;
  private ProgressHelper progressHelper;

  public FilterByWordInputProcessor(ProcessorContext context) {
    super(context);
  }

  @Override
  public void initialize() throws Exception {
    Configuration conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    filterWord = conf.get(FilterLinesByWord.FILTER_PARAM_NAME);
    if (filterWord == null) {
      getContext().reportFailure(TaskFailureType.NON_FATAL, null, "No filter word specified");
    }
  }

  @Override
  public void handleEvents(List<Event> processorEvents) {
    throw new UnsupportedOperationException("Not expecting any events to the broadcast processor");
  }

  @Override
  public void close() throws Exception {
    if (progressHelper != null) {
      progressHelper.shutDownProgressTaskService();
    }
  }

  @Override
  public void run(Map<String, LogicalInput> _inputs,
                  Map<String, LogicalOutput> _outputs) throws Exception {
    this.inputs = _inputs;
    this.outputs = _outputs;
    this.progressHelper = new ProgressHelper(this.inputs, getContext(), this.getClass().getSimpleName());
    if (_inputs.size() != 1) {
      throw new IllegalStateException("FilterByWordInputProcessor processor can only work with a single input");
    }

    if (_outputs.size() != 1) {
      throw new IllegalStateException("FilterByWordInputProcessor processor can only work with a single output");
    }

    for (LogicalInput input : _inputs.values()) {
      input.start();
    }
    for (LogicalOutput output : _outputs.values()) {
      output.start();
    }

    LogicalInput li = _inputs.values().iterator().next();
    if (!(li instanceof MRInput)) {
      throw new IllegalStateException("FilterByWordInputProcessor processor can only work with MRInput");
    }

    LogicalOutput lo = _outputs.values().iterator().next();
    if (!(lo instanceof UnorderedKVOutput)) {
      throw new IllegalStateException("FilterByWordInputProcessor processor can only work with OnFileUnorderedKVOutput");
    }
    progressHelper.scheduleProgressTaskService(0, 100);
    MRInputLegacy mrInput = (MRInputLegacy) li;
    mrInput.init();
    UnorderedKVOutput kvOutput = (UnorderedKVOutput) lo;

    Configuration updatedConf = mrInput.getConfigUpdates();
    Text srcFile = new Text();
    srcFile.set("UNKNOWN_FILENAME_IN_PROCESSOR");
    if (updatedConf != null) {
      String fileName = updatedConf.get(MRJobConfig.MAP_INPUT_FILE);
      if (fileName != null) {
        LOG.info("Processing file: " + fileName);
        srcFile.set(fileName);
      }
    }

    KeyValueReader kvReader = mrInput.getReader();
    KeyValueWriter kvWriter = kvOutput.getWriter();

    while (kvReader.next()) {
      Object key = kvReader.getCurrentKey();
      Object val = kvReader.getCurrentValue();

      Text valText = (Text) val;
      String readVal = valText.toString();
      if (readVal.contains(filterWord)) {
        LongWritable lineNum = (LongWritable) key;
        TextLongPair outVal = new TextLongPair(srcFile, lineNum);
        kvWriter.write(valText, outVal);
      }
    }
  }
}
