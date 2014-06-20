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

package org.apache.tez.runtime.library.common;

import org.apache.hadoop.classification.InterfaceAudience.Private;


public class Constants {

  // TODO NEWTEZ Check which of these constants are expecting specific pieces of information which are being removed - like taskAttemptId
  
  public static final String TEZ = "tez";

  public static final String MAP_OUTPUT_FILENAME_STRING = "file.out";
  public static final String MAP_OUTPUT_INDEX_SUFFIX_STRING = ".index";
  public static final String REDUCE_INPUT_FILE_FORMAT_STRING = "%s/map_%d.out";

  public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;
  public static String MERGED_OUTPUT_PREFIX = ".merged";

  public static final long DEFAULT_COMBINE_RECORDS_BEFORE_PROGRESS = 10000;

  // TODO NEWTEZ Remove this constant once the old code is removed.
  public static final String TEZ_RUNTIME_TASK_ATTEMPT_ID = 
      "tez.runtime.task.attempt.id";

  public static final String TEZ_RUNTIME_TASK_OUTPUT_FILENAME_STRING =
      "file.out";

  public static final String TEZ_RUNTIME_TASK_OUTPUT_INDEX_SUFFIX_STRING =
      ".index";

  public static final String TEZ_RUNTIME_TASK_INPUT_FILE_FORMAT_STRING =
      "%s/task_%d.out"; 

  public static final String TEZ_RUNTIME_JOB_CREDENTIALS =
      "tez.runtime.job.credentials";
  
  @Private
  public static final String TEZ_RUNTIME_TASK_MEMORY =
      "tez.runtime.task.memory";
  
  public static final String TEZ_RUNTIME_TASK_OUTPUT_DIR = "output";
  
  public static final String TEZ_RUNTIME_TASK_OUTPUT_MANAGER = 
      "tez.runtime.task.local.output.manager";

}
