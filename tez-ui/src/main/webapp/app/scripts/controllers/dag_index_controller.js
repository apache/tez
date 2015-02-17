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

 //TODO: watch individual counters.
App.DagIndexController = Em.ObjectController.extend({
	controllerName: 'DagIndexController',

	taskIconStatus: function() {
		return App.Helpers.misc.getStatusClassForEntity(this.get('model.status'));
	}.property('id', 'status', 'counterGroups'),

  progressStr: function() {
    var pct;
    if (Ember.typeOf(this.get('progress')) === 'number') {
      pct = App.Helpers.number.fractionToPercentage(this.get('progress'));
    }
    return pct;
  }.property('id', 'status', 'progress'),

	totalTasks: function() {
		return App.Helpers.misc.getCounterValueForDag(this.get('counterGroups'), 
			this.get('id'), 'org.apache.tez.common.counters.DAGCounter', 'TOTAL_LAUNCHED_TASKS')
	}.property('id', 'counterGroups'),

	sucessfulTasks: function() {
		return App.Helpers.misc.getCounterValueForDag(this.get('counterGroups'), this.get('id'),
			'org.apache.tez.common.counters.DAGCounter', 'NUM_SUCCEEDED_TASKS')
	}.property('id', 'counterGroups'),

	failedTasks: function() {
		return App.Helpers.misc.getCounterValueForDag(this.get('counterGroups'), this.get('id'),
			'org.apache.tez.common.counters.DAGCounter', 'NUM_FAILED_TASKS')
	}.property('id', 'counterGroups'),

	killedTasks: function() {
		return App.Helpers.misc.getCounterValueForDag(this.get('counterGroups'), this.get('id'),
			'org.apache.tez.common.counters.DAGCounter', 'NUM_KILLED_TASKS')
	}.property('id', 'counterGroups'),

	hasFailedTasks: function() {
		return this.get('failedTasks') > 0;
	}.property('id', 'counterGroups'),

  failedTasksLink: function() {
    return '#tasks?status=FAILED&parentType=TEZ_DAG_ID&parentID=' + this.get('id');
  }.property()

});
