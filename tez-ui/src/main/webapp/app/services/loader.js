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

import { assert, inspect } from '@ember/debug';
import EmberObject from '@ember/object';
import { assign } from '@ember/polyfills';
import Service, { inject as service } from '@ember/service';
import { dasherize } from '@ember/string';
import { getOwner } from '@ember/application';

export default Service.extend({

  nameSpace: '',
  store: service('store'),
  cache: null,

  _setOptions: function (options) {
    var nameSpace = options.nameSpace;
    if(nameSpace) {
      // We need to validate only if nameSpace is passed. Else it would be stored in the global space
      assert(`Invalid nameSpace. Please pass a string instead of ${inspect(nameSpace)}`, typeof nameSpace === 'string');
      this.set("nameSpace", nameSpace);
    }
  },

  init: function (options) {
    this._super(...arguments);
    this._setOptions(options || {});
    this.set("cache", EmberObject.create());
  },

  checkRequisite: function (type) {
    var store = this.store,
        adapter = store.adapterFor(type),
        serializer = store.serializerFor(type);

    assert(
      `No loader adapter found for type ${type}. Either extend loader and create a custom adapter or extend ApplicationAdapter from loader.`,
      adapter && adapter._isLoader
    );
    assert(
      `No loader serializer found for type ${type}. Either extend loader and create a custom serializer or extend ApplicationSerializer from loader.`,
      serializer && serializer._isLoader
    );
  },

  lookup: function (type, name, options) {
    name = dasherize(name);
    return getOwner(this).lookup(type + ":" + name, options);
  },

  entityFor: function (entityName) {
    var entity = this.lookup("entitie", entityName);
    if(!entity) {
      entity = this.lookup("entitie", "entity", { singleton: false });
      entity.set("name", entityName);
    }
    return entity;
  },

  getCacheKey: function (type, query, id) {
    var parts = [type];

    if(id) {
      parts.push(id);
    }
    if(query) {
      parts.push(JSON.stringify(query));
    }

    return parts.join(":");
  },

  loadNeed: function (record, needName, options, queryParams, urlParams) {
    var entity = this.entityFor(record.get("constructor.modelName"));
    return entity.loadNeed(this, record, needName, options, queryParams, urlParams);
  },

  normalizeOptions: function (options) {
    options = options || {};

    if(!options.cache){
      options = assign({}, options);
      options.cache = options.reload ? EmberObject.create() : this.cache;
    }

    return options;
  },

  queryRecord: function(type, id, options, query, urlParams) {
    var entity = this.entityFor(type),
        cacheKey = this.getCacheKey(type, query, id),
        record;

    this.checkRequisite(type);

    options = this.normalizeOptions(options);

    record = options.cache.get(cacheKey);
    if(record) {
      return record;
    }

    record = entity.queryRecord(this, id, options, query, urlParams);
    options.cache.set(cacheKey, record);

    return record;
  },
  query: function(type, query, options, urlParams) {
    var entity = this.entityFor(type),
        cacheKey = this.getCacheKey(type, query),
        records;

    this.checkRequisite(type);

    options = this.normalizeOptions(options);

    records = options.cache.get(cacheKey);
    if(records) {
      return records;
    }

    records = entity.query(this, query, options, urlParams);
    options.cache.set(cacheKey, records);

    return records;
  },

  unloadAll: function (type, skipID) {
    var store = this.store,
        loaderNS = this.nameSpace;

    store.peekAll(type).forEach(function (record) {
      var id = record.get("id");

      if(id.substr(0, id.indexOf(":")) === loaderNS && record.get("entityID") !== skipID) {
        store.unloadRecord(record);
      }
    });
  },
});
