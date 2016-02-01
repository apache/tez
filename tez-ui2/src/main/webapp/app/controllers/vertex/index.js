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

import Ember from 'ember';

import PageController from '../page';

function taskLinkComputerFactory(name) {
  return Ember.computed(name, function () {
    var tasks = this.get(name);

    if(tasks) {
      return tasks.map(function (task) {
        return {
          routeName: 'task',
          model: task,
          text: task
        };
      });
    }
  });
}

export default PageController.extend({

  pathname: Ember.computed(function() {
    return window.location.pathname;
  }).volatile(),

  firstTasksToStart: taskLinkComputerFactory("model.firstTasksToStart"),
  lastTasksToFinish: taskLinkComputerFactory("model.lastTasksToFinish"),
  shortestDurationTasks: taskLinkComputerFactory("model.shortestDurationTasks"),
  longestDurationTasks: taskLinkComputerFactory("model.longestDurationTasks"),

});
