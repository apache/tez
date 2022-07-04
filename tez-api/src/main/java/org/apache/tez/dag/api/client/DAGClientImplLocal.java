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
package org.apache.tez.dag.api.client;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

import javax.annotation.Nullable;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;

/**
 * A DAGClientImpl which is typically used for tez.local.mode.without.network=true.
 */
public class DAGClientImplLocal extends DAGClientImpl {

  private BiFunction<Set<StatusGetOpts>, Long, DAGStatus> dagStatusFunction;

  public DAGClientImplLocal(ApplicationId appId, String dagId, TezConfiguration conf,
                            FrameworkClient frameworkClient, UserGroupInformation ugi,
                            BiFunction<Set<StatusGetOpts>, Long, DAGStatus> dagStatusFunction) {
    super(appId, dagId, conf, frameworkClient, ugi);
    this.dagStatusFunction = dagStatusFunction;
  }

  @Override
  protected DAGStatus getDAGStatusInternal(@Nullable Set<StatusGetOpts> statusOptions, long timeout)
      throws TezException, IOException {
    return dagStatusFunction.apply(statusOptions == null ? new HashSet<>() : statusOptions,
        timeout);
  }
}
