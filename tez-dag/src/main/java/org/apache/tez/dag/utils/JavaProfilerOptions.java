package org.apache.tez.dag.utils;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

/**
 * Placeholder to store JVM profiler related information
 */
public class JavaProfilerOptions {
  private static final Log LOG = LogFactory.getLog(JavaProfilerOptions.class);

  //To check any characters apart from "a-zA-Z_0-9 : ; , [] space" anywhere in input.
  final static Pattern INVALID_TASKS_TO_PROFILE_REGEX = Pattern
    .compile("[^(\\w\\s;:,\\[\\])]");

  //vertex name can not have [a-zA-Z_0-9] and space. Task id is expected to be a number
  //: is used for specifying task id range. , is used as task id separator.
  final static Pattern TASKS_TO_PROFILE_REGEX = Pattern
    .compile("([\\w\\s]+)\\[([\\d:,\\s]*)\\];?");

  //Range regex where ':' should always be prepended and appended with digit.
  final static Pattern RANGE_REGEX = Pattern.compile("(\\d+):(\\d+)");

  private Map<String, BitSet> tasksToProfileMap;
  private String jvmProfilingOpts;

  public JavaProfilerOptions(Configuration conf) {
    jvmProfilingOpts =
        conf.getTrimmed(TezConfiguration.TEZ_PROFILE_JVM_OPTS, "");

    if (!Strings.isNullOrEmpty(jvmProfilingOpts)) {
      this.tasksToProfileMap = getTasksToProfile(conf);
      if (!tasksToProfileMap.isEmpty() && jvmProfilingOpts.isEmpty()) {
        LOG.warn(TezConfiguration.TEZ_PROFILE_JVM_OPTS
            + " should be specified for profiling");
      }
    }
  }

  /**
   * Get profiler options for a specific task in the vertex
   *
   * @param jvmOpts
   *          vertex specific JVM option
   * @param vertexName
   * @param taskIdx
   * @return jvmProfilingOpts
   */
  public String getProfilerOptions(String jvmOpts, String vertexName,
      int taskIdx) {
    jvmOpts = (jvmOpts == null) ? "" : jvmOpts;
    vertexName = vertexName.replaceAll(" ", "");
    String result =
        jvmProfilingOpts.replaceAll("__VERTEX_NAME__", vertexName)
          .replaceAll("__TASK_INDEX__", Integer.toString(taskIdx));
    result = (jvmOpts + " " + result);

    LOG.info("Profiling option added to vertexName=" + vertexName
        + ", taskIdx=" + taskIdx + ", jvmOpts=" + result.trim());

    return result.trim();
  }

  /**
   * Find if the JVM needs profiling
   *
   * @param vertexName
   * @param taskId
   * @return boolean
   */
  public boolean shouldProfileJVM(String vertexName, int taskId) {
    if (tasksToProfileMap == null || taskId < 0) {
      return false;
    }
    BitSet taskSet = tasksToProfileMap.get(vertexName);
    // profile all tasks in the vertex, if taskSet is empty
    return (taskSet == null) ? false : ((taskSet.isEmpty()) ? true : taskSet.get(taskId));
  }

  /**
   * <pre>
   * Get the set of tasks to be profiled in the job. Example formats are
   * v[0,1,2] - To profile subset of tasks in a vertex
   * v[1,2,3];v2[5,6,7] - To profile multiple vertices
   * v[1:5,20,30];v2[2:5,60,7] - To support range of tasks in vertices. Partial
   * ranges are not supported (e.g v[:5],v2[2:]).
   * v[] - To profile all tasks in a vertex
   * </pre>
   *
   * @param conf
   * @return Map<String, BitSet>
   */
  private Map<String, BitSet> getTasksToProfile(Configuration conf) {
    String tasksToProfile =
        conf.getTrimmed(TezConfiguration.TEZ_PROFILE_TASK_LIST, "");
    final Map<String, BitSet> resultSet = new HashMap<String, BitSet>();
    if (tasksToProfile.isEmpty() || !isValid(tasksToProfile)) {
      return resultSet; // empty set
    }
    Matcher matcher = TASKS_TO_PROFILE_REGEX.matcher(tasksToProfile);
    while (matcher.find()) {
      String vertexName = matcher.group(1).trim();
      BitSet profiledTaskSet = parseTasksToProfile(matcher.group(2).trim());
      resultSet.put(vertexName, profiledTaskSet);
    }
    LOG.info("Tasks to profile info=" + resultSet);
    return resultSet;
  }

  private boolean isValid(String tasksToProfile) {
    if (INVALID_TASKS_TO_PROFILE_REGEX.matcher(tasksToProfile).find()) {
      LOG.warn("Invalid option specified, "
          + TezConfiguration.TEZ_PROFILE_TASK_LIST + "=" + tasksToProfile);
      return false;
    }
    return true;
  }

  /**
   * Get the set of tasks to be profiled within a vertex
   *
   * @param tasksToProfileInVertex
   * @return Set<Integer> containing the task indexes to be profiled
   */
  private BitSet parseTasksToProfile(String tasksToProfileInVertex) {
    BitSet profiledTaskSet = new BitSet();
    if (Strings.isNullOrEmpty(tasksToProfileInVertex)) {
      return profiledTaskSet;
    }
    Iterable<String> tasksInVertex =
        Splitter.on(",").omitEmptyStrings().trimResults().split(tasksToProfileInVertex);
    for (String task : tasksInVertex) {
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
          profiledTaskSet.set(i);
        }
      } else {
        profiledTaskSet.set(Integer.parseInt(task.trim()));
      }
    }
    return profiledTaskSet;
  }
}