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

package org.apache.tez.dag.app.rm.container;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.app.ContainerContext;

import org.apache.tez.common.Preconditions;
import org.apache.tez.common.ContainerSignatureMatcher;

public class ContainerContextMatcher implements ContainerSignatureMatcher {

  private void checkArguments(Object cs1, Object cs2) {
    Objects.requireNonNull(cs1, "Arguments cannot be null");
    Objects.requireNonNull(cs2, "Arguments cannot be null");
    Preconditions.checkArgument(cs1 instanceof ContainerContext
        && cs2 instanceof ContainerContext,
        "Container context can only compare instances of "
            + ContainerContext.class.getName() + ", Recevied: "
            + cs1.getClass().getName() + " and " + cs2.getClass().getName());

  }
  
  @Override
  public boolean isSuperSet(Object cs1, Object cs2) {
    checkArguments(cs1, cs2);
    ContainerContext context1 = (ContainerContext) cs1;
    ContainerContext context2 = (ContainerContext) cs2;

    return context1.isSuperSet(context2);
  }

  @Override
  public boolean isExactMatch(Object cs1, Object cs2) {
    checkArguments(cs1, cs2);
    ContainerContext context1 = (ContainerContext) cs1;
    ContainerContext context2 = (ContainerContext) cs2;

    return context1.isExactMatch(context2);
  }

  @Override
  public Map<String, LocalResource> getAdditionalResources(Map<String, LocalResource> lr1,
      Map<String, LocalResource> lr2) {
    Objects.requireNonNull(lr1);
    Objects.requireNonNull(lr2);

    Map<String, LocalResource> c2LocalResources = new HashMap<String, LocalResource>(lr2);
    for (Entry<String, LocalResource> c1LocalResource : lr1.entrySet()) {
      c2LocalResources.remove(c1LocalResource.getKey());
    }
    return c2LocalResources;
  }

  @Override
  public Object union(Object cs1, Object cs2) {
    checkArguments(cs1, cs2);
    return ContainerContext.union((ContainerContext) cs1, (ContainerContext) cs2);
  }

}
