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

import EmberObject from '@ember/object';
import { assign } from '@ember/polyfills';
import { assert } from '@ember/debug';
import { all } from 'rsvp';
import NameMixin from '../mixins/name';
import MoreObject from '../utils/more-object';

var Entity = EmberObject.extend(NameMixin, {

  queryRecord: function (loader, id, options, query, urlParams) {
    var that = this;
    return this.store.queryRecord(this.name, {
      id: id,
      nameSpace: loader.get('nameSpace'),
      params: query,
      urlParams: urlParams
    }).then(function (record) {
      return that._loadAllNeeds(loader, record, options, urlParams);
    });
  },

  query: function (loader, query, options, urlParams) {
    var that = this;
    return this.store.query(this.name, {
      nameSpace: loader.get('nameSpace'),
      params: query,
      urlParams: urlParams
    }).then(function (records) {
      for (let i = 0, len = records.length; i < len; ++i) {
        var record = records.objectAt(i);
        that._loadAllNeeds(loader, record, options, urlParams);
      }
      return records;
    });
  },

  normalizeNeed: function(name, needOptions, parentModel, queryParams, urlParams) {
    var need = {
      name: name,
      type: name,
      idKey: "",

      loadType: "", // Possible values lazy, demand
      silent: false,

      //urlParams
      //queryParams
    },
    overrides = {};

    if(typeof needOptions === 'object') {
      if(MoreObject.isFunction(needOptions.urlParams)) {
        overrides.urlParams = needOptions.urlParams.call(needOptions, parentModel);
      }
      if(MoreObject.isFunction(needOptions.queryParams)) {
        overrides.queryParams = needOptions.queryParams.call(needOptions, parentModel);
      }

      overrides.idKey = needOptions.idKey;
      overrides = assign({}, needOptions, overrides);
    }
    else if(typeof needOptions === 'string') {
      overrides.idKey = needOptions;
    }

    if(typeof overrides.idKey === 'string') {
      overrides.withID = true;
      overrides.id = parentModel.get(overrides.idKey);
    }

    if(queryParams) {
      overrides.queryParams = assign({}, overrides.queryParams, queryParams);
    }
    if(urlParams) {
      overrides.urlParams = assign({}, overrides.urlParams, urlParams);
    }

    return assign({}, need, overrides);
  },

  setNeed: function (parentModel, name, model) {
    if(!parentModel.get("isDeleted")) {
      parentModel.set(name, model);
    }
    return parentModel;
  },

  _loadNeed: function (loader, parentModel, needOptions, options, index) {
    var needLoader,
        that = this,
        types = needOptions.type,
        type;

    if(!Array.isArray(types)) {
      types = [types];
    }

    index = index || 0;
    type = types[index];

    if(needOptions.withID) {
      needLoader = loader.queryRecord(
        type,
        needOptions.id,
        options,
        needOptions.queryParams,
        needOptions.urlParams
      );
    }
    else {
      needLoader = loader.query(
        type,
        needOptions.queryParams,
        options,
        needOptions.urlParams
      );
    }

    needLoader = needLoader.then(function (model) {
      that.setNeed(parentModel, needOptions.name, model);
      return model;
    });

    needLoader = needLoader.catch(function (err) {
      if(++index < types.length) {
        return that._loadNeed(loader, parentModel, needOptions, options, index);
      }

      if(needOptions.silent) {
        that.setNeed(parentModel, needOptions.name, null);
      }
      else {
        throw(err);
      }
    });

    return needLoader;
  },

  loadNeed: function (loader, parentModel, needName, options, queryParams, urlParams) {
    var needOptions = parentModel.get(`needs.${needName}`);
    assert(`Need '${needName}' not defined in model!`, needOptions);

    needOptions = this.normalizeNeed(needName, needOptions, parentModel, queryParams, urlParams);
    return this._loadNeed(loader, parentModel, needOptions, options);
  },

  _loadAllNeeds: function (loader, model, options/*, urlParams*/) {
    var needsPromise = this.loadAllNeeds(loader, model, options);

    if(needsPromise) {
      return needsPromise.then(function () {
        return model;
      });
    }

    return model;
  },

  loadAllNeeds: function (loader, parentModel, options, queryParams, urlParams) {
    var needLoaders = [],
        needs = parentModel.get("needs");

    if(needs) {
      let keys = Object.keys(needs);
      let length = keys.length;
      for (var i = 0; i < length; i++) {
        var name = keys[i];
        var needOptions = needs[name];
        var loadNeed;

        if(MoreObject.isFunction(needOptions.loadType)) {
          needOptions.loadType = needOptions.loadType.call(needOptions, parentModel);
        }

        loadNeed = needOptions.loadType !== "demand";

        if(options && options.demandNeeds) {
          loadNeed = options.demandNeeds.indexOf(name) !== -1;
        }

        if(loadNeed) {
          // we can delay normalizing needs that won't be loaded for performance reasons
          needOptions = this.normalizeNeed(name, needOptions, parentModel, queryParams, urlParams);
          // load required needs
          let needLoader = this._loadNeed(loader, parentModel, needOptions, options);

          if(needOptions.loadType !== "lazy") {
            needLoaders.push(needLoader);
          }
        }
        else {
          // reset on demand needs
          parentModel.set(name, null);
        }
      };
    }
    // note the completion time of the fully loaded model
    parentModel.refreshLoadTime();

    if(needLoaders.length) {
      return all(needLoaders);
    }
  },
});

export default Entity;
