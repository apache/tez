/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.analyzer;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.datamodel.DagInfo;


public interface Analyzer {

  /**
   * Analyze Dag
   *
   * @param dagInfo
   * @throws TezException
   */
  public void analyze(DagInfo dagInfo) throws TezException;

  /**
   * Get the result of analysis
   *
   * @return analysis result
   * @throws TezException
   */
  public Result getResult() throws TezException;

  /**
   * Get name of the analyzer
   *
   * @return name of analyze
   */
  public String getName();

  /**
   * Get description of the analyzer
   *
   * @return description of analyzer
   */
  public String getDescription();

  /**
   * Get config properties related to this analyzer
   *
   * @return config related to analyzer
   */
  public Configuration getConfiguration();
}
