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

package org.apache.hadoop.mapreduce.v2.app2.lazy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.app2.AppContext;
import org.apache.hadoop.mapreduce.v2.app2.job.Job;
import org.apache.hadoop.mapreduce.v2.app2.job.impl.NotRunningJob;
import org.apache.hadoop.mapreduce.v2.app2.rm.ContainerRequestor;
import org.apache.hadoop.mapreduce.v2.app2.rm.RMContainerAllocator;

public class LazyRMContainerAllocator extends RMContainerAllocator {

  private static final Log LOG = LogFactory.getLog(
      LazyRMContainerAllocator.class);

  public LazyRMContainerAllocator(ContainerRequestor requestor,
      AppContext appContext) {
    super(requestor, appContext);
  }

  @Override
  protected synchronized void assignContainers() {
    Job j = getJob();
    if (j == null || j instanceof NotRunningJob) {
      LOG.debug("Nothing to do in assignContainers as no job submitted");
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Job submitted. Assigning containers");
    }
    super.assignContainers();
  }

}
