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

App.AbstractEntity = DS.Model.extend({
  // type of the entity. should be one of App.EntityType
  entityType: DS.attr('string'),
  timeStamp: null,

  didLoad: function () {
    this.set('timeStamp', new Date());
  },

  observeReloading: function () {
    if(!this.get('isReloading')) {
      this.didLoad();
    }
  }.observes('isReloading')
});

App.EntityType = {
  DAG: 'dag',
  VERTEX: 'vertex',
  TASK: 'task',
  TASK_ATTEMPT: 'task_attempt',
};
