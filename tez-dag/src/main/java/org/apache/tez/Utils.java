/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.tez.dag.app.AppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
/**
 * Utility class within the tez-dag module
 */
public class Utils {

  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  public static String getContainerLauncherIdentifierString(int launcherIndex, AppContext appContext) {
    String name;
    try {
      name = appContext.getContainerLauncherName(launcherIndex);
    } catch (Exception e) {
      LOG.error("Unable to get launcher name for index: " + launcherIndex +
          ", falling back to reporting the index");
      return "[" + String.valueOf(launcherIndex) + "]";
    }
    return "[" + launcherIndex + ":" + name + "]";
  }

  public static String getTaskCommIdentifierString(int taskCommIndex, AppContext appContext) {
    String name;
    try {
      name = appContext.getTaskCommunicatorName(taskCommIndex);
    } catch (Exception e) {
      LOG.error("Unable to get taskcomm name for index: " + taskCommIndex +
          ", falling back to reporting the index");
      return "[" + String.valueOf(taskCommIndex) + "]";
    }
    return "[" + taskCommIndex + ":" + name + "]";
  }

  public static String getTaskSchedulerIdentifierString(int schedulerIndex, AppContext appContext) {
    String name;
    try {
      name = appContext.getTaskSchedulerName(schedulerIndex);
    } catch (Exception e) {
      LOG.error("Unable to get scheduler name for index: " + schedulerIndex +
          ", falling back to reporting the index");
      return "[" + String.valueOf(schedulerIndex) + "]";
    }
    return "[" + schedulerIndex + ":" + name + "]";
  }

}
