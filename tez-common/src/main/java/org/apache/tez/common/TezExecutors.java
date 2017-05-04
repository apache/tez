/*
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

package org.apache.tez.common;

import java.util.concurrent.ExecutorService;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Interface to capture factory of ExecutorService.
 */
@Private
@Unstable
public interface TezExecutors {

  /**
   * Create a ExecutorService with the given parameters.
   *
   * @param parallelism Represents total number of tasks to be executed in parallel.
   * @param threadNameFormat The name the thread should take when executing tasks from this executor
   * @return An ExecutorService.
   */
  ExecutorService createExecutorService(int parallelism, String threadNameFormat);

  /**
   * Shutdown all the ExecutorService created using this factory.
   */
  void shutdown();

  /**
   * Shutdown all the ExecutorService created using this factory. It will discard any tasks which
   * are not running and interrupt the running tasks.
   */
  void shutdownNow();
}
