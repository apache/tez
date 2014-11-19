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

App.TaskAttempt = App.AbstractEntity.extend({

  // start time of the entity
  startTime: DS.attr('number'),

  // end time of the entity
  endTime: DS.attr('number'),

	entityType: App.EntityType.TASK_ATTEMPT,


	// container
	containerId: DS.attr('string'),
  nodeId: DS.attr('string'),

  // status of the task attempt
	status: DS.attr('string'),

  taskID: DS.attr('string'),
  vertexID: DS.attr('string'),
  dagID: DS.attr('string'),

  inProgressLog: DS.attr('string'),
  completedLog: DS.attr('string'),

  counterGroups: DS.hasMany('counterGroup', { inverse: 'parent' })
});
