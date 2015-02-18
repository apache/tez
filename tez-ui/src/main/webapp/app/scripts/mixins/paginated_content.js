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

App.PaginatedContentMixin = Em.Mixin.create({
  // paging related values. These are bound automatically to the values in url. via the queryParams
  // defined in the route.
  count: 10,

  page: 1,
  fromID: null,

  // The dropdown contents for number of items to show.
  countOptions: [5, 10, 25, 50, 100],

  isRefreshable: true,

  /* There is currently no efficient way in ATS to get pagination data, so we fake one.
   * store the first dag id on a page so that we can navigate back and store the last one 
   * (not shown on page to get the id where next page starts)
   */
  navIDs: {
    prevIDs: [],
    currentID: undefined,
    nextID: undefined
  },

  queryParams: {
    count: true,
  },

  entities: [],
  _paginationFilters: {},
  loading: true,

  load: function() {
    this.resetNavigation();
    this.loadEntities();
  }.observes('count'),

  sortedContent: function() {
    // convert to a ArrayController. we do not sort at this point as the data is
    // not globally sorted, and the total number of elements in array is unknown
    var sorted = Em.ArrayController.create({
      model: this.get('entities')
    });
    this.updatePagination(sorted.toArray());
    return sorted.slice(0, this.count);
  }.property('entities', 'numEntities'),

  updateLoading: function () {
    this.set('loading', false);
  },

  loadEntities: function() {
    var that = this;
    var childEntityType = this.get('childEntityType');
    var defaultErrMsg = 'Error while loading %@. could not connect to %@'
      .fmt(childEntityType, App.env.timelineBaseUrl);


    that.set('loading', true);

    this.get('store').unloadAll(childEntityType);
    this.get('store').findQuery(childEntityType, this.getFilterProperties()).then(function(entities){
      that.set('entities', entities);
      var loaders = [];
      try {
        var loader = Em.tryInvoke(that, 'loadAdditional');
        if (!!loader) {
          loaders.push(loader);
        }
      } catch(error) {
        Em.Logger.error("Exception invoking additional load", error);
      }
      Em.RSVP.allSettled(loaders).then(function(){
        that.updateLoading();
      });
    }).catch(function(error){
      Em.Logger.error(error);
      var err = App.Helpers.misc.formatError(error, defaultErrMsg);
      var msg = 'error code: %@, message: %@'.fmt(err.errCode, err.msg);
      App.Helpers.ErrorBar.getInstance().show(msg, err.details);
    });
  },

  setFiltersAndLoadEntities: function(filters) {
    this._paginationFilters = filters;
    this.load();
  },

  resetNavigation: function() {
    this.set('navIDs.prevIDs', []);
    this.set('navIDs.currentID', '');
    this.set('navIDs.nextID', '');
    this.set('fromID', null);
    this.set('page', 1);
  },

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
  }.property('navIDs.prevIDs.[]', 'navIDs.prevIDs.length', 'fromID', 'page'),

  hasNext: function() {
    return !!this.navIDs.nextID;
  }.property('navIDs.nextID'),

  actions:{
    // go to previous page
    navigatePrev: function () {
      var prevPageId = this.navIDs.prevIDs.popObject();
      this.set('fromID', prevPageId);
      this.set('loading', true);
      this.set('page', this.get('page') - 1);
      this.loadEntities();
    },

    refresh: function () {
      this.load();
    },

    // goto first page.
    navigateFirst: function() {
      var firstPageId = this.navIDs.prevIDs[0];
      this.set('navIDs.prevIDs', []);
      this.set('fromID', firstPageId);
      this.set('loading', true);
      this.set('page', 1);
      this.loadEntities();
    },

    // go to next page
    navigateNext: function () {
      this.navIDs.prevIDs.pushObject(this.navIDs.currentID);
      this.set('fromID', this.get('navIDs.nextID'));
      this.set('loading', true);
      this.set('page', this.get('page') + 1);
      this.loadEntities();
    },
  },

  _concatFilters: function(obj) {
    var p = [];
    for(var k in obj) {
      if (!Em.empty(obj[k])) {
        p.push(k + ':' + obj[k]);
      }
    }
    return p.join(',');
  },

  getFilterProperties: function() {
    var params = {
      limit: this.count + 1
    };

    var f = this._paginationFilters;
    var primary = f.primary;
    var secondary = f.secondary || {};

    primary = this._concatFilters(primary);

    secondary = this._concatFilters(secondary);

    if (!Em.empty(primary)) {
      params['primaryFilter'] = primary;
    }

    if (!Em.empty(secondary)) {
      params['secondaryFilter'] = secondary;
    }

    if (!Em.empty(this.fromID)) {
      params['fromId'] = this.fromID;
    }

    return params;
  },
});
