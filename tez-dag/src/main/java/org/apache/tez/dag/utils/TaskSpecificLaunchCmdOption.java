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

package org.apache.tez.dag.utils;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.TezClientUtils;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

/**
 * Placeholder to store additional launch command options to be specified for specific tasks in
 * different vertices.
 */
public class TaskSpecificLaunchCmdOption {
  private static final Log LOG = LogFactory.getLog(TaskSpecificLaunchCmdOption.class);

  //To check any characters apart from "a-zA-Z_0-9 : ; , [] space" anywhere in input.
  final static Pattern INVALID_TASK_NAME_REGEX = Pattern
    .compile("[^(\\w\\s;:,\\[\\])]");

  /**
   * Regex to validate the task ranges. Vertex name can only have [a-zA-Z_0-9] and
   * space. Task id is expected to be a number. : is used for specifying task id range. , is used
   * as task id separator.
   */
  final static Pattern TASKS_REGEX = Pattern
    .compile("([\\w\\s]+)\\[([\\d:,\\s]*)\\];?");

  //Range regex where ':' should always be prepended and appended with digit.
  final static Pattern RANGE_REGEX = Pattern.compile("(\\d+):(\\d+)");

  private final Map<String, BitSet> tasksMap;
  //Task specific launch-cmd options
  private final String tsLaunchCmdOpts;
  //Task specific log options
  private final String[] tsLogParams;

  public TaskSpecificLaunchCmdOption(Configuration conf) {
    this.tsLaunchCmdOpts =
        conf.getTrimmed(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS);
    this.tsLogParams = TezClientUtils
        .parseLogParams(conf.getTrimmed(TezConfiguration.TEZ_TASK_SPECIFIC_LOG_LEVEL));

    if (shouldParseSpecificTaskList()) {
      this.tasksMap = getSpecificTasks(conf);
    } else {
      this.tasksMap = null;
    }
  }

  /**
   * Get task specific command options in a vertex
   *
   * @param launchCmdOpts
   *          vertex specific command option
   * @param vertexName
   * @param taskIdx
   * @return tsLaunchCmdOpts
   */
  public String getTaskSpecificOption(String launchCmdOpts, String vertexName,
      int taskIdx) {
    if (this.tsLaunchCmdOpts != null) {
      launchCmdOpts = (launchCmdOpts == null) ? "" : launchCmdOpts;
      vertexName = vertexName.replaceAll(" ", "");
      String result =
          this.tsLaunchCmdOpts.replaceAll("__VERTEX_NAME__", vertexName)
              .replaceAll("__TASK_INDEX__", Integer.toString(taskIdx));
      result = (launchCmdOpts + " " + result);

      LOG.info("Launch-cmd options added to vertexName=" + vertexName
          + ", taskIdx=" + taskIdx + ", tsLaunchCmdOpts=" + result.trim());

      return result.trim();
    }
    return launchCmdOpts;
  }

  public boolean hasModifiedTaskLaunchOpts() {
    return !Strings.isNullOrEmpty(tsLaunchCmdOpts);
  }

  /**
   * Retrieve a parsed form of the log string specified for per-task usage. </p>
   * The first element of the array is the general log level. </p>
   * The second level, if it exists, is the additional per logger configuration.
   *
   * @return parsed form of the log string specified. null if none specified
   */
  public String[] getTaskSpecificLogParams() {
    return this.tsLogParams;
  }

  public boolean hasModifiedLogProperties() {
    return this.tsLogParams != null;
  }

  /**
   * Find if task specific launch command options have to be added.
   *
   * @param vertexName
   * @param taskId
   * @return boolean
   */
  public boolean addTaskSpecificLaunchCmdOption(String vertexName, int taskId) {
    if (tasksMap == null || taskId < 0) {
      return false;
    }
    BitSet taskSet = tasksMap.get(vertexName);
    // profile all tasks in the vertex, if taskSet is empty
    return (taskSet == null) ? false : ((taskSet.isEmpty()) ? true : taskSet.get(taskId));
  }

  private boolean shouldParseSpecificTaskList() {
    return !(Strings.isNullOrEmpty(tsLaunchCmdOpts) && tsLogParams == null);
  }

  /**
   * <pre>
   * Get the set of tasks in the job, which needs task specific launch command arguments to be
   * added. Example formats are
   * v[0,1,2] - Add additional options to subset of tasks in a vertex
   * v[1,2,3];v2[5,6,7] - Add additional options in tasks in multiple vertices
   * v[1:5,20,30];v2[2:5,60,7] - To support range of tasks in vertices. Partial
   * ranges are not supported (e.g v[:5],v2[2:]).
   * v[] - For all tasks in a vertex
   * </pre>
   *
   * @param conf
   * @return a map from the vertex name to a BitSet representing tasks to be instruemented. null if
   *         the provided configuration is empty or invalid
   */
  private Map<String, BitSet> getSpecificTasks(Configuration conf) {
    String specificTaskList =
        conf.getTrimmed(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS_LIST);
    if (!Strings.isNullOrEmpty(specificTaskList) && isValid(specificTaskList)) {
      final Map<String, BitSet> resultSet = new HashMap<String, BitSet>();
      Matcher matcher = TASKS_REGEX.matcher(specificTaskList);
      while (matcher.find()) {
        String vertexName = matcher.group(1).trim();
        BitSet taskSet = parseTasks(matcher.group(2).trim());
        resultSet.put(vertexName, taskSet);
      }
      LOG.info("Specific tasks with additional launch-cmd options=" + resultSet);
      return resultSet;
    } else {
      return null;
    }
  }

  private boolean isValid(String specificTaskList) {
    if (INVALID_TASK_NAME_REGEX.matcher(specificTaskList).find()) {
      LOG.warn("Invalid option specified, "
          + TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS_LIST + "=" + specificTaskList);
      return false;
    }
    return true;
  }

  /**
   * Get the set of tasks that need additional launch command options within a vertex
   *
   * @param tasksInVertex
   * @return Set<Integer> containing the task indexes to be profiled
   */
  private BitSet parseTasks(String tasksInVertex) {
    BitSet taskSet = new BitSet();
    if (Strings.isNullOrEmpty(tasksInVertex)) {
      return taskSet;
    }
    Iterable<String> tasks =
        Splitter.on(",").omitEmptyStrings().trimResults().split(tasksInVertex);
    for (String task : tasks) {
      /**
       * TODO: this is horrible way to check the ranges.
       * Should use RangeSet when guava is upgraded.  Also, need to support partial
       * ranges like "1:", ":50".  With current implementation partial ranges are not
       * allowed.
       */
      if (task.endsWith(":") || task.startsWith(":")) {
       //invalid range. e.g :20, 6: are not supported.
        LOG.warn("Partial range is considered as an invalid option");
        return null;
      }
      Matcher taskMatcher = RANGE_REGEX.matcher(task);
      if (taskMatcher.find()) {
        int start = Integer.parseInt((taskMatcher.group(1).trim()));
        int end = Integer.parseInt((taskMatcher.group(2).trim()));
        for (int i = Math.min(start, end); i <= Math.max(start, end); i++) {
          taskSet.set(i);
        }
      } else {
        taskSet.set(Integer.parseInt(task.trim()));
      }
    }
    return taskSet;
  }
}