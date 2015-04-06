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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.tez.dag.app.dag.Vertex;

import com.google.common.base.Preconditions;

public class ContainerContext {

  private static final Log LOG = LogFactory.getLog(ContainerContext.class);

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
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cannot match container: Additional local resource needed is not of type FILE"
              + ", resourceName: " + additionalLREntry.getKey()
              + ", resourceDetails: " + additionalLREntry);
        }
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

  /**
   * Create a new ContainerContext to account for container re-use. On re-use, there is
   * re-localization of additional LocalResources. Also, a task from a different vertex could be
   * run on the given container.
   *
   * Only a merge of local resources is needed as:
   *
   * credentials are modified at run-time based on the task spec.
   * the environment for a container cannot be changed. A re-used container is always
   * expected to have a super-set.
   * javaOpts have to be identical for re-use.
   *
   * Vertex should be overridden to account for the new task being scheduled to run on this
   * container context.
   *
   * @param c1 ContainerContext 1 Original task's context
   * @param c2 ContainerContext 2 Newly assigned task's context
   * @return Merged ContainerContext
   */
  public static ContainerContext union(ContainerContext c1, ContainerContext c2) {
    HashMap<String, LocalResource> mergedLR = new HashMap<String, LocalResource>();
    mergedLR.putAll(c1.getLocalResources());
    mergedLR.putAll(c2.getLocalResources());
    ContainerContext union = new ContainerContext(mergedLR, c1.credentials, c1.environment,
        c1.javaOpts, c2.vertex);
    return union;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("LocalResources: [");
    if (localResources != null) {
      for (Map.Entry<String, LocalResource> lr : localResources.entrySet()) {
        sb.append("[ name=")
            .append(lr.getKey())
            .append(", value=")
            .append(lr.getValue())
            .append("],");
      }
    }
    sb.append("], environment: [");
    if (environment != null) {
      for (Map.Entry<String, String> entry : environment.entrySet()) {
        sb.append("[ ").append(entry.getKey()).append("=").append(entry.getValue())
            .append(" ],");
      }
    }
    sb.append("], credentials(token kinds): [");
    if (credentials != null) {
      for (Token<? extends TokenIdentifier> t : credentials.getAllTokens()) {
        sb.append(t.getKind().toString())
            .append(",");
      }
    }
    sb.append("], javaOpts: ")
      .append(javaOpts)
      .append(", vertex: ")
      .append(( vertex == null ? "null" : vertex.getLogIdentifier()));

    return sb.toString();
  }

}
