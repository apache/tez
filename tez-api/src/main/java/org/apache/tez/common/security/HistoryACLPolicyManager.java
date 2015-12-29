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

package org.apache.tez.common.security;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.security.HistoryACLPolicyException;

/**
 * ACL Policy Manager
 * An instance of this implements any ACL related activity when starting a session or
 * submitting a DAG
 */
@Unstable
@Private
public interface HistoryACLPolicyManager extends Configurable {

  /**
   * Take any necessary steps for setting up Session ACLs
   * @param conf Configuration
   * @param applicationId Application ID for the session
   * @throws Exception
   */
  public Map<String, String> setupSessionACLs(Configuration conf, ApplicationId applicationId)
      throws IOException, HistoryACLPolicyException;

  /**
   * Take any necessary steps for setting up ACLs for an AM which is running in non-session mode
   * @param conf Configuration
   * @param applicationId Application ID for the AM
   * @param dagAccessControls ACLs defined for the DAG being submitted
   * @throws Exception
   */
  public Map<String, String> setupNonSessionACLs(Configuration conf, ApplicationId applicationId,
      DAGAccessControls dagAccessControls) throws IOException, HistoryACLPolicyException;

  /**
   * Take any necessary steps for setting up ACLs for a DAG that is submitted to a Session
   * @param conf Configuration
   * @param applicationId Application ID for the AM
   * @param dagAccessControls ACLs defined for the DAG being submitted
   * @throws Exception
   */
  public Map<String, String> setupSessionDAGACLs(Configuration conf, ApplicationId applicationId,
      String dagName, DAGAccessControls dagAccessControls) throws IOException, HistoryACLPolicyException;


  public void updateTimelineEntityDomain(Object timelineEntity, String domainId);

  /**
   * Call this to stop and clean up
   */
  public void close();
}
