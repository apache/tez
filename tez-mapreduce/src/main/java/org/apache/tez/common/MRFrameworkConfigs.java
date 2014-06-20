/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.tez.common;

public class MRFrameworkConfigs {

  /**
   * One local dir for the specific job.
   */

  private static final String MR_FRAMEWORK_PREFIX = "tez.mr.framework.";

  /**
   * The directory which contains the localized files for this task.
   */
  public static final String TASK_LOCAL_RESOURCE_DIR = MR_FRAMEWORK_PREFIX + "task-local-resource.dir";
  public static final String TASK_LOCAL_RESOURCE_DIR_DEFAULT = "/tmp";

  public static final String JOB_LOCAL_DIR = MR_FRAMEWORK_PREFIX + "job.local.dir";
}
