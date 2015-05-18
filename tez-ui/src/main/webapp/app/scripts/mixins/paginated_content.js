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
  rowCount: 10,

  page: 1,
  fromID: null,

  // The dropdown contents for number of items to show.
  rowCountOptions: [5, 10, 25, 50, 100],
  maxRowCount: function () {
    return Math.max.apply(null, this.get('rowCountOptions'));
  }.property('rowCountOptions'),

  isRefreshable: true,

  /* There is currently no efficient way in ATS to get pagination data, so we fake one.
   * store the first dag id on a page so that we can navigate back and store the last one 
   * (not shown on page to get the id where next page starts)
   */
  navIDs: [],

  queryParams: {
    rowCount: true,
  },

  entities: [],
  _paginationFilters: {},
  loading: true,

  load: function() {
    this.resetNavigation();
    this.loadEntities();
  }.observes('rowCount'),

  lastPage: function () {
    return this.get('navIDs.length') + 1;
  }.property('navIDs.length'),

  sortedContent: function() {
    // convert to a ArrayController. we do not sort at this point as the data is
    // not globally sorted, and the total number of elements in array is unknown
    var sorted = Em.ArrayController.create({
      model: this.get('entities')
    });
    this.updatePagination(sorted.toArray());
    return sorted.slice(0, this.rowCount);
  }.property('entities', 'numEntities'),

  updateLoading: function () {
    this.set('loading', false);
  },

  loadEntities: function() {
    var that = this;
    var childEntityType = this.get('childEntityType');
    var defaultErrMsg = 'Error while loading %@.'
      .fmt(childEntityType);


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
    this.set('navIDs', []);
    this.set('fromID', null);
    this.set('page', 1);
  },

  updatePagination: function(dataArray) {
    var nextFromId = null,
        navIDs = this.get('navIDs'),
        rowCount = this.get('rowCount');

    if(dataArray && dataArray.length == rowCount + 1) {
      nextFromId = dataArray.objectAt(rowCount).get('id');
      if (navIDs.indexOf(nextFromId) == -1 &&
          this.get('page') >= navIDs.get('length')) {
          navIDs.pushObject(nextFromId);
      }
    }
  },

  actions:{
    refresh: function () {
      this.load();
    },

    changePage: function (pageNum) {
      this.set('fromID', this.get('navIDs.' + (pageNum - 2)) || null);
      this.set('loading', true);
      this.set('page', pageNum);
      this.loadEntities();
    }
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
      limit: Math.min(this.rowCount, this.get('maxRowCount')) + 1
    };

    var f = this._paginationFilters;
    var primary = f.primary || {};
    var secondary = f.secondary || {};

    // TimelineRest API allows only one primaryFilter but any number of
    // secondary filters. secondary filters are first checked in otherInfo
    // field and then in primaryFilter field. this is tricky (for ex. when
    // otherInfo and primaryFilter has same key). so we move all filters
    // other than first non null primary to secondary.
    var foundOnePrimaryFilter = false;
    $.each(primary, function(name, value) {
      if (!value) {
        delete primary[name];
        return true;
      }
      if (foundOnePrimaryFilter) {
        secondary[name] = value;
        delete primary[name];
      }
      foundOnePrimaryFilter = true;
    });

    primary = this._concatFilters(primary);
    secondary = this._concatFilters(secondary);

    if (!Em.empty(primary)) {
      params['primaryFilter'] = primary;
    }

    if (!Em.empty(secondary)) {
      params['secondaryFilter'] = secondary;
    }

    if (!Em.empty(this.get('fromID'))) {
      params['fromId'] = this.get('fromID');
    }

    return params;
  },
});
