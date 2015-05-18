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

var dataCache = {};

App.DataArrayLoaderMixin = Em.Mixin.create({
  data: [],
  loading: false,

  entityType: null,
  filterEntityType: null,
  filterEntityId: null,

  // At a time for an entity type, records under one single domain value will only be cached.
  cacheDomain: undefined,

  isRefreshable: true,

  _cacheKey: function () {
    return [
      this.get('filterEntityType'),
      this.get('filterEntityId'),
      this.get('entityType'),
    ].join(':');
  }.property('filterEntityType', 'filterEntityId', 'entityType'),

  getFilter: function (limit) {
    return {
      limit: limit || App.Helpers.number.MAX_SAFE_INTEGER,
      primaryFilter: '%@:%@'.fmt(
        App.Helpers.misc.getTimelineFilterForType(this.get('filterEntityType')),
        this.get('filterEntityId')
      )
    };
  },

  loadData: function (skipCache) {
    var data;

    if(this.get('loading')) {
      return false;
    }

    if(!skipCache) {
      data = dataCache[this.get('_cacheKey')];
    }

    if(data && data.get('content.length')) {
      this.set('data', data);
    }
    else {
      this.loadAllData();
    }

    return true;
  },

  loadAllData: function () {
    this.set('loading', true);

    // Load all rows
    return this.beforeLoad().
      then(this.load.bind(this, this.getFilter())).
      then(this.afterLoad.bind(this)).
      then(this.cacheData.bind(this)).
      then(this.set.bind(this, 'loading', false)).
      catch(this.errorHandler.bind(this));
  },

  beforeLoad: function () {
    return new Em.RSVP.resolve();
  },

  load: function (filter) {
    var entityType = this.get('entityType'),
        store = this.get('store'),
        data = dataCache[this.get('_cacheKey')],
        domainKey = entityType + ":Domain";

    if(this.get('cacheDomain') != dataCache[domainKey]) {
      store.unloadAll(entityType);
      dataCache[domainKey] = this.get('cacheDomain');
    }
    else if(data) {
      data.toArray().forEach(function (record) {
        record.unloadRecord();
      });
      dataCache[this.get('_cacheKey')] = null;
    }

    return store.findQuery(entityType, filter).
      then(this.set.bind(this, 'data')).
      catch(this.errorHandler.bind(this));
  },

  cacheData: function () {
    dataCache[this.get('_cacheKey')] = this.get('data');
  },

  errorHandler: function (error) {
    Em.Logger.error(error);
    var err = App.Helpers.misc.formatError(error, 'Error while loading ' + this.get('entityType'));
    var msg = 'Error code: %@, message: %@'.fmt(err.errCode, err.msg);
    App.Helpers.ErrorBar.getInstance().show(msg, err.details);
  },

  afterLoad: function () {
    return new Em.RSVP.resolve();
  },
});
