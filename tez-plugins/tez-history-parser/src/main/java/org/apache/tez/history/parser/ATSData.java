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

package org.apache.tez.history.parser;

import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.codehaus.jettison.json.JSONException;

import static org.apache.hadoop.classification.InterfaceStability.Evolving;

/**
 * Main interface to pull data from ATS.
 * <p/>
 * It is possible that to have ATS data store in any DB (e.g LevelDB or HBase).  Depending on
 * store, there can be multiple implementations to pull data from these stores and create the
 * DagInfo object for analysis.
 * <p/>
 */
@Evolving
public interface ATSData {

  /**
   * Get the DAG representation for processing
   *
   * @param dagId
   * @return DagInfo
   * @throws JSONException
   * @throws TezException
   */
  public DagInfo getDAGData(String dagId) throws TezException;

}
