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

package org.apache.tez.dag.app.rm;

import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.rm.TaskScheduler.ContainerSignatureMatcher;

import com.google.common.base.Preconditions;

public class ContainerContextMatcher implements ContainerSignatureMatcher {

  @Override
  public boolean isCompatible(Object cs1, Object cs2) {
    Preconditions.checkNotNull(cs1, "Arguments cannot be null");
    Preconditions.checkNotNull(cs2, "Arguments cannot be null");
    Preconditions.checkArgument(cs1 instanceof ContainerContext
        && cs2 instanceof ContainerContext,
        "Container context can only compare instances of "
            + ContainerContext.class.getName() + ", Recevied: "
            + cs1.getClass().getName() + " and " + cs2.getClass().getName());

    ContainerContext context1 = (ContainerContext) cs1;
    ContainerContext context2 = (ContainerContext) cs2;

    return context1.isSuperSet(context2);
  }
}