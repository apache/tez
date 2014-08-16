/*
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
package org.apache.tez.mapreduce.processor;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.Processor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.collect.Lists;

/**
 * A {@link SimpleProcessor} that provides Map Reduce specific post
 * processing by calling commit (if needed) on all {@link MROutput}s 
 * connected to this {@link Processor}. 
 */
@Public
@Evolving
public abstract class SimpleMRProcessor extends SimpleProcessor {
  private static final Log LOG = LogFactory.getLog(SimpleMRProcessor.class);

  public SimpleMRProcessor(ProcessorContext context) {
    super(context);
  }

  @Override
  protected void postOp() throws Exception {
    if (getOutputs() == null) {
      return; // No post op
    }
    List<MROutput> mrOuts = Lists.newLinkedList();
    for (LogicalOutput output : getOutputs().values()) {
      if (output instanceof MROutput) {
        MROutput mrOutput = (MROutput) output;
        mrOutput.flush();
        if (mrOutput.isCommitRequired()) {
          mrOuts.add((MROutput) output);
        }
      }
    }
    if (mrOuts.size() > 0) {
      // This will loop till the AM asks for the task to be killed. As
      // against, the AM sending a signal to the task to kill itself
      // gracefully. The AM waits for the current committer to successfully
      // complete and then kills us. Until then we wait in case the
      // current committer fails and we get chosen to commit.
      while (!getContext().canCommit()) {
        Thread.sleep(100);
      }
      boolean willAbort = false;
      Exception savedEx = null;
      for (MROutput output : mrOuts) {
        try {
          output.commit();
        } catch (IOException ioe) {
          LOG.warn("Error in committing output", ioe);
          willAbort = true;
          savedEx = ioe;
          break;
        }
      }
      if (willAbort == true) {
        for (MROutput output : mrOuts) {
          try {
            output.abort();
          } catch (IOException ioe) {
            LOG.warn("Error in aborting output", ioe);
          }
        }
        throw savedEx;
      }
    }
  }
}
