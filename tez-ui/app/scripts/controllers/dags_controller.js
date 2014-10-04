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

App.DagsController = Em.ArrayController.extend({
	controllerName: "DagsController",

	pageTitle: "Dags",

	pageSubTitle: "All Dags",

  /* filtering and sorting related */
  queryParams: {
    count: true,
    fromID: true
  },

  count: 10,

  fromID: '',

  fromTS: '',

  fields: 'events,primaryfilters,otherinfo',
  /* end sort & filter related */

  // content is being loaded.
  loading: true,

  /* There is currently no efficient way in ATS to get pagination data, so we fake one.
   * store the first dag id on a page so that we can navigate back and store the last one 
   * (not shown on page to get the id where next page starts)
   */
  navIDs: {
    prevIDs: [],
    currentID: undefined,
    nextID: undefined
  },

  updateLoading: function() {
    this.set('loading', false);
  }.observes('content'),

  sortedContent: function() {
    var sorted = Em.ArrayController.create({
      model: this.get('content'),
      sortProperties: ['startTime'],
      sortAscending: false
    });
    this.updatePagination(sorted.toArray());
    return sorted.slice(0, this.count);
  }.property('content.isUpdating', 'content.isLoading'),

  updatePagination: function(currentPageDagIDs) {
    if (!!currentPageDagIDs && currentPageDagIDs.length > 0) {
      this.set('navIDs.currentID', currentPageDagIDs[0].id);
      var nextID = undefined;
      if (currentPageDagIDs.length > this.count) {
        // save the last id, so that we can use that as firt id on next page.
        nextID = currentPageDagIDs[this.count].id;
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
      this.transitionToRoute('dags');
    },

    // goto first page.
    navigateFirst: function() {
      var firstPageId = this.navIDs.prevIDs[0];
      this.set('navIDs.prevIDs', []);
      this.set('fromID', firstPageId);
      this.set('loading', true);
      this.transitionToRoute('dags');
    },

    // go to next page
    navigateNext: function () {
      this.navIDs.prevIDs.pushObject(this.navIDs.currentID);
      this.set('fromID', this.get('navIDs.nextID'));
      this.set('loading', true);
      this.transitionToRoute('dags');
    },
  },

  getFilterParams: function(params) {
    var filterParams = {
      limit: (parseInt(params.count) || this.get('count')) + 1,
      fields: this.get('fields')
    };
    var fromID = params.fromID || this.get('fromID'), 
        fromTS = params.fromTS || this.get('fromTS'),
        user = params.user;
    
    if (fromID) {
      filterParams['fromId'] = fromID; 
    }

    if (fromTS) {
      filterParams['fromTs'] = fromTS;
    }

    if (user) {
      filterParams['primaryFilter'] = 'user:' + user;
    }

    return filterParams;
  },

	/* table view for dags */
  columns: function() {
    var idColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Dag Id',
      tableCellViewClass: Em.Table.TableCell.extend({
      	template: Em.Handlebars.compile("{{#link-to 'dag' view.cellContent class='ember-table-content'}}{{view.cellContent}}{{/link-to}}")
      }),
      getCellContent: function(row) {
      	return row.get('id');
      }
    });
    var nameColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Name',
      getCellContent: function(row) {
      	return row.get('name');
      }
    });
    var userColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Submitter',
      getCellContent: function(row) {
      	return row.get('user');
      }
    });
    var statusColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Status',
      getCellContent: function(row) {
      	return row.get('status');
      }
    });
    var startTimeColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'SubmissionTime',
      getCellContent: function(row) {
      	return App.Helpers.date.dateFormat(row.get('startTime'));
      }
    });
    var appIdColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Application Id',
      getCellContent: function(row) {
      	return row.get('applicationId');
      }
    });
    return [idColumn, nameColumn, userColumn, statusColumn, startTimeColumn, appIdColumn];
  }.property(),


});