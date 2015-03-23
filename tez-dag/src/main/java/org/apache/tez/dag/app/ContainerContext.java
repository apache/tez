/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.dag.app;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.tez.dag.app.dag.Vertex;

import com.google.common.base.Preconditions;

public class ContainerContext {

  private static final Logger LOG = LoggerFactory.getLogger(ContainerContext.class);

  private final Map<String, LocalResource> localResources;
  private final Credentials credentials;
  private final Map<String, String> environment;
  private final String javaOpts;
  private final Vertex vertex;

  // FIXME Add support for service meta data comparisons

  public ContainerContext(Map<String, LocalResource> localResources,
      Credentials credentials, Map<String, String> environment, String javaOpts) {
    Preconditions.checkNotNull(localResources,
        "localResources should not be null");
    Preconditions.checkNotNull(credentials, "credentials should not be null");
    Preconditions.checkNotNull(environment, "environment should not be null");
    Preconditions.checkNotNull(javaOpts, "javaOpts should not be null");
    this.localResources = localResources;
    this.credentials = credentials;
    this.environment = environment;
    this.javaOpts = javaOpts;
    this.vertex = null;
  }
  
  public ContainerContext(Map<String, LocalResource> localResources,
      Credentials credentials, Map<String, String> environment, String javaOpts,
      @Nullable Vertex vertex) {
    Preconditions.checkNotNull(localResources,
        "localResources should not be null");
    Preconditions.checkNotNull(credentials, "credentials should not be null");
    Preconditions.checkNotNull(environment, "environment should not be null");
    Preconditions.checkNotNull(javaOpts, "javaOpts should not be null");
    this.localResources = localResources;
    this.credentials = credentials;
    this.environment = environment;
    this.javaOpts = javaOpts;
    this.vertex = vertex;
  }

  public Map<String, LocalResource> getLocalResources() {
    return this.localResources;
  }

  public Credentials getCredentials() {
    return this.credentials;
  }

  public Map<String, String> getEnvironment() {
    return this.environment;
  }

  public String getJavaOpts() {
    return this.javaOpts;
  }

  /**
   * @return true if this ContainerContext is a super-set of the specified
   *         container context.
   */
  public boolean isSuperSet(ContainerContext otherContext) {
    Preconditions.checkNotNull(otherContext, "otherContext should not null");
    // Assumptions:
    // Credentials are the same for all containers belonging to a DAG.
    // Matching can be added if containers are used across DAGs

    // Match javaOpts
    if (!this.javaOpts.equals(otherContext.javaOpts)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Incompatible java opts, "
          + ", this=" + this.javaOpts
          + ", other=" + otherContext.javaOpts);
      }
      return false;
    }

    return isSuperSet(this.environment, otherContext.getEnvironment(), "Environment")
        && localResourcesCompatible(this.localResources, otherContext.getLocalResources());
  }
  
  /**
   * @return true if this ContainerContext is an exact match of the specified
   *         container context.
   */
  public boolean isExactMatch(ContainerContext otherContext) {
    return (this.vertex == otherContext.vertex);
  }

  // TODO Once LRs are handled via YARN, remove this check - and ensure
  // YarnTezDAGChild knows how to handle the additional types in terms of
  // classpath modification
  private static boolean localResourcesCompatible(Map<String, LocalResource> srcLRs,
      Map<String, LocalResource> reqLRs) {
    Map<String, LocalResource> reqLRsCopy = new HashMap<String, LocalResource>(reqLRs);
    for (Entry<String, LocalResource> srcLREntry : srcLRs.entrySet()) {
      LocalResource requestedLocalResource = reqLRsCopy.remove(srcLREntry.getKey());
      if (requestedLocalResource != null && !srcLREntry.getValue().equals(requestedLocalResource)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cannot match container: Attempting to use same target resource name: "
              + srcLREntry.getKey()
              + ", but with different source resources. Already localized: "
              + srcLREntry.getValue() + ", requested: " + requestedLocalResource);
        }
        return false;
      }
    }
    for (Entry<String, LocalResource> additionalLREntry : reqLRsCopy.entrySet()) {
      LocalResource lr = additionalLREntry.getValue();
      if (EnumSet.of(LocalResourceType.ARCHIVE, LocalResourceType.PATTERN).contains(lr.getType())) {
        return false;
      }
    }
    return true;
  }

  private static <K, V> boolean isSuperSet(Map<K, V> srcMap, Map<K, V> matchMap,
      String matchInfo) {
    for (Entry<K, V> oEntry : matchMap.entrySet()) {
      K oKey = oEntry.getKey();
      V oVal = oEntry.getValue();
      if (srcMap.containsKey(oKey)) {
        if (!oVal.equals(srcMap.get(oKey))) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Incompatible container context"
              + ", matchInfo=" + matchInfo
              + ", thisKey=" + oKey
              + ", thisVal=" + srcMap.get(oKey)
              + ", otherVal=" + oVal);
          }
          return false;
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Incompatible container context"
            + ", matchInfo=" + matchInfo
            + ", thisKey=" + oKey
            + ", thisVal=null"
            + ", otherVal=" + oVal);
        }
        return false;
      }
    }
    return true;
  }
}
