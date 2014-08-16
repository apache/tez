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

package org.apache.tez.runtime.library.processor;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.runtime.api.Processor;
import org.apache.tez.runtime.api.ProcessorContext;

/**
 * Built-in convenience {@link Processor} to be used for pre-warming.
 * If this is customized by the user then they need to make sure that
 * the custom class jar is localized for the prewarm vertex and other
 * vertices that need to take advantage of prewarming
 *
 */
@Unstable
@Public
public class PreWarmProcessor extends SimpleProcessor {

  public PreWarmProcessor(ProcessorContext context) {
    super(context);
  }

  @Override
  public void run() throws Exception {
    preWarmTezCode();
    preWarmUserCode();
  }
  
  /**
   * Pre-warm Tez code. Users can override this with an empty method
   * to not pre-warm Tez code if they want to.
   */
  protected void preWarmTezCode() {
    // Do nothing. Can potentially pre-warm Tez library components
    
    // Currently, must sleep for some time so that container re-use
    // can be prevented from kicking in. This will allow sufficient 
    // time to obtain containers from YARN as long as those resources 
    // are available
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Pre-warm user code. Users can override this 
   * to pre-warm their own code if they want to. 
   */
  protected void preWarmUserCode() {
    // Do nothing
  }

}
