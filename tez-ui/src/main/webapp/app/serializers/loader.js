/*global more*/
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

import Ember from 'ember';
import DS from 'ember-data';

var MoreObject = more.Object;

// TODO - Move to more js
function mapObject(hash, map, thisArg) {
  var mappedObject = Ember.Object.create();
  MoreObject.forEach(map, function (key, value) {
    if(MoreObject.isString(value)) {
      mappedObject.set(key, Ember.get(hash, value));
    }
    else if (MoreObject.isFunction(value)) {
      mappedObject.set(key, value.call(thisArg, hash));
    }
    else {
      Ember.assert("Unknown mapping value");
    }
  });
  return mappedObject;
}

export default DS.JSONSerializer.extend({
  _isLoader: true,

  mergedProperties: ["maps"],
  maps: null,

  extractId: function (modelClass, resourceHash) {
    var id = this._super(modelClass, resourceHash.data),
        nameSpace = resourceHash.nameSpace;

    if(nameSpace) {
      return nameSpace + ":" + id;
    }
    return id;
  },
  extractAttributes: function (modelClass, resourceHash) {
    var maps = this.get('maps'),
        data = resourceHash.data;
    return this._super(modelClass, maps ? mapObject(data, maps, this) : data);
  },
  extractRelationships: function (modelClass, resourceHash) {
    return this._super(modelClass, resourceHash.data);
  },

  extractSinglePayload: function (payload) {
    return payload;
  },
  extractArrayPayload: function (payload) {
    return payload;
  },

  normalizeSingleResponse: function (store, primaryModelClass, payload, id, requestType) {
    payload.data = this.extractSinglePayload(payload.data);
    return this._super(store, primaryModelClass, payload, id, requestType);
  },

  normalizeArrayResponse: function (store, primaryModelClass, payload, id, requestType) {
    var nameSpace = payload.nameSpace;

    // convert into a _normalizeResponse friendly format
    payload = this.extractArrayPayload(payload.data);
    Ember.assert("Loader expects an array in return for a query", Array.isArray(payload));
    payload = payload.map(function (item) {
      return {
        nameSpace: nameSpace,
        data: item
      };
    });

    return this._super(store, primaryModelClass, payload, id, requestType);
  }
});
