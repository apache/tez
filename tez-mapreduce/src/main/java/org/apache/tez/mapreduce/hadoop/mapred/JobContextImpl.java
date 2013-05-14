/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tez.mapreduce.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.mapreduce.hadoop.IDConverter;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobContextImpl 
    extends org.apache.hadoop.mapreduce.task.JobContextImpl 
    implements JobContext {
  private JobConf job;
  private Progressable progress;

  public JobContextImpl(JobConf conf, TezDAGID dagId,
                 Progressable progress) {
    super(conf, IDConverter.toMRJobId(dagId));
    this.job = conf;
    this.progress = progress;
  }

  public JobContextImpl(JobConf conf, TezDAGID dagId) {
    this(conf, dagId, Reporter.NULL);
  }
  
  /**
   * Get the job Configuration
   * 
   * @return JobConf
   */
  public JobConf getJobConf() {
    return job;
  }
  
  /**
   * Get the progress mechanism for reporting progress.
   * 
   * @return progress mechanism 
   */
  public Progressable getProgressible() {
    return progress;
  }
}
