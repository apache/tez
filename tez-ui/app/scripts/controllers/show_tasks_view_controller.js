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
var parentTypeToATSType = {
	dag: 'TEZ_DAG_ID',
	task: 'TEZ_TASK_ID',
	taskAttempt: 'TEZ_TASK_ATTEMPT_ID'
};

var childEntityTypeForParent = {
	dag: 'task',
	task: 'taskAttempt'
}

//TODO: This is generic view controller - name it so.
App.ShowTasksViewController = Em.ObjectController.extend({
	controllerName: 'ShowTasksViewController',

	count: 10,

  fromID: '',

  fromTS: '',

  /* There is currently no efficient way in ATS to get pagination data, so we fake one.
   * store the first dag id on a page so that we can navigate back and store the last one 
   * (not shown on page to get the id where next page starts)
   */
  navIDs: {
    prevIDs: [],
    currentID: undefined,
    nextID: undefined
  },

	entities: [],
	loading: true,

  getChildEntityType: function(parentType) {
    return childEntityTypeForParent[parentType];
  },

	sortedContent: function() {
    var sorted = Em.ArrayController.create({
      model: this.get('entities'),
      sortProperties: ['startTime'],
      sortAscending: false
    });
    this.updatePagination(sorted.toArray());
    return sorted.slice(0, this.count);
  }.property('entities'),

	loadEntities: function() {
		var that = this;
		var parentEntityType = this.get('parentEntityType');
		var childEntityType = childEntityTypeForParent[parentEntityType];
		this.get('store').unloadAll(childEntityType);
		this.get('store').findQuery(childEntityType, this.getFilterProperties()).then(function(entities){
			that.set('entities', entities);
			that.set('loading', false);
		}).catch(function(jqXHR){
			alert('failed');
		});
	}.observes('content.parentEntityID', 'fromID'),

	updatePagination: function(dataArray) {
    if (!!dataArray && dataArray.get('length') > 0) {
      this.set('navIDs.currentID', dataArray.objectAt(0).get('id'));
      var nextID = undefined;
      if (dataArray.get('length') > this.count) {
        // save the last id, so that we can use that as firt id on next page.
        nextID = dataArray.objectAt(this.count).get('id');
      }
      this.set('navIDs.nextID', nextID);
    }
  },

  hasPrev: function() {
    return this.navIDs.prevIDs.length > 0;
  }.property('navIDs.prevIDs.[]'),

  hasNext: function() {
    return !!this.navIDs.nextID;
  }.property('navIDs.nextID'),

  actions:{
    // go to previous page
    navigatePrev: function () {
      var prevPageId = this.navIDs.prevIDs.popObject();
      this.set('fromID', prevPageId);
      this.set('loading', true);
    },

    // goto first page.
    navigateFirst: function() {
      var firstPageId = this.navIDs.prevIDs[0];
      this.set('navIDs.prevIDs', []);
      this.set('fromID', firstPageId);
      this.set('loading', true);
    },

    // go to next page
    navigateNext: function () {
      this.navIDs.prevIDs.pushObject(this.navIDs.currentID);
      this.set('fromID', this.get('navIDs.nextID'));
      this.set('loading', true);
    },
  },

	getFilterProperties: function() {
		var params = {
			primaryFilter: parentTypeToATSType[this.get('parentEntityType')] + ':' + this.get('parentEntityID'),
			limit: this.count + 1
		};

		if (this.fromID) {
			params['fromId'] = this.fromID;
		}

		return params;
	},
});