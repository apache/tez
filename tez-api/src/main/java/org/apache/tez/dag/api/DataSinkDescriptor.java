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

package org.apache.tez.dag.api;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.security.Credentials;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Defines the output and output committer for a data sink 
 *
 */
@Public
public class DataSinkDescriptor {
  private final OutputDescriptor outputDescriptor;
  private final OutputCommitterDescriptor committerDescriptor;
  
  private final Credentials credentials;
  private final Collection<URI> urisForCredentials = Sets.newHashSet();

  /**
   * Create a {@link DataSinkDescriptor}
   * @param outputDescriptor
   *          An {@link OutputDescriptor} for the output
   * @param committerDescriptor
   *          Specify a committer to be used for the output. Can be null. After all
   *          tasks in the vertex (or in the DAG) have completed, the committer
   *          (if specified) is invoked to commit the outputs. Commit is a data
   *          sink specific operation that usually determines the visibility of
   *          the output to external observers. E.g. moving output files from
   *          temporary dirs to the real output dir. When there are multiple
   *          executions of a task, the commit process also helps decide which
   *          execution will be included in the final output. Users should
   *          consider whether their application or data sink need a commit
   *          operation.
   * @param credentials Credentials needs to access the data sink
   */
  @Deprecated
  public DataSinkDescriptor(OutputDescriptor outputDescriptor,
      @Nullable OutputCommitterDescriptor committerDescriptor,
      @Nullable Credentials credentials) {
    this.outputDescriptor = outputDescriptor;
    this.committerDescriptor = committerDescriptor;
    this.credentials = credentials;
  }

  /**
   * Create a {@link DataSinkDescriptor}
   * @param outputDescriptor
   *          An {@link OutputDescriptor} for the output
   * @param committerDescriptor
   *          Specify a committer to be used for the output. Can be null. After all
   *          tasks in the vertex (or in the DAG) have completed, the committer
   *          (if specified) is invoked to commit the outputs. Commit is a data
   *          sink specific operation that usually determines the visibility of
   *          the output to external observers. E.g. moving output files from
   *          temporary dirs to the real output dir. When there are multiple
   *          executions of a task, the commit process also helps decide which
   *          execution will be included in the final output. Users should
   *          consider whether their application or data sink need a commit
   *          operation.
   * @param credentials Credentials needs to access the data sink
   */
  public static DataSinkDescriptor create(OutputDescriptor outputDescriptor,
      @Nullable OutputCommitterDescriptor committerDescriptor,
      @Nullable Credentials credentials) {
    return new DataSinkDescriptor(outputDescriptor, committerDescriptor, credentials);
  }
  
  /**
   * Get the {@link OutputDescriptor} for this {@link DataSinkDescriptor}
   * @return {@link OutputDescriptor}
   */
  public OutputDescriptor getOutputDescriptor() {
    return outputDescriptor;
  }
  
  /**
   * Get the {@link OutputCommitterDescriptor} for this {@link DataSinkDescriptor}
   * @return {@link OutputCommitterDescriptor}
   */
  public @Nullable OutputCommitterDescriptor getOutputCommitterDescriptor() {
    return committerDescriptor;
  }
  
  /** 
  * This method can be used to specify a list of URIs for which Credentials
  * need to be obtained so that the job can run. An incremental list of URIs
  * can be provided by making multiple calls to the method.
  * 
  * Currently, @{link credentials} can only be fetched for HDFS and other
  * {@link org.apache.hadoop.fs.FileSystem} implementations that support
  * credentials.
  * 
  * @param uris
  *          a list of {@link URI}s
  * @return this
  */
  public synchronized DataSinkDescriptor addURIsForCredentials(Collection<URI> uris) {
    Preconditions.checkNotNull(uris, "URIs cannot be null");
    urisForCredentials.addAll(uris);
    return this;
  }
  
  /**
   * Get the URIs for which credentials will be obtained
   * @return an unmodifiable list representing the URIs for which credentials
   *         are required.
   */
  public Collection<URI> getURIsForCredentials() {
    return Collections.unmodifiableCollection(urisForCredentials);
  }

  /**
   * Get the {@link Credentials} for this {@link DataSinkDescriptor}
   * @return {@link Credentials}
   */
  public @Nullable Credentials getCredentials() {
    return credentials;
  }

}
