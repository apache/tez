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

public class VertexLocationHint  {

  private int numTasks;
  private TaskLocationHint[] taskLocationHints;

  public VertexLocationHint() {
    this(0);
  }

  public VertexLocationHint(int numTasks) {
    this(numTasks, new TaskLocationHint[numTasks]);
  }

  public VertexLocationHint(int numTasks,
      TaskLocationHint[] taskLocationHints) {
    this.numTasks = numTasks;
    this.taskLocationHints = taskLocationHints;
  }

  public int getNumTasks() {
    return numTasks;
  }

  public TaskLocationHint[] getTaskLocationHints() {
    return taskLocationHints;
  }

  public void setTaskLocationHints(TaskLocationHint[] taskLocationHints) {
    this.taskLocationHints = taskLocationHints;
  }

  public static class TaskLocationHint {

    // Host names if any to be used
    private String[] hosts;
    // Rack names if any to be used
    private String[] racks;

    public TaskLocationHint() {
      this(new String[0], new String[0]);
    }

    public TaskLocationHint(String[] hosts, String[] racks) {
      this.hosts = hosts;
      this.racks = racks;
    }

    public String[] getDataLocalHosts() {
      return hosts;
    }
    public void setDataLocalHosts(String[] hosts) {
      this.hosts = hosts;
    }
    public String[] getRacks() {
      return racks;
    }
    public void setRacks(String[] racks) {
      this.racks = racks;
    }
  }
}
