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

var MoreObject = more.Object;

export default Ember.Object.extend({

  loadRelations: function (loader, model) {
    var needsPromise = this.loadNeeds(loader, model);

    if(needsPromise) {
      return needsPromise.then(function () {
        return model;
      });
    }

    return model;
  },

  normalizeNeed: function(name, options) {
    var need = {
      name: name,
      type: name,
      idKey: options,
      lazy: false,
      silent: false
    };

    if(typeof options === 'object') {
      return Ember.Object.create(need, options);
    }

    return Ember.Object.create(need);
  },

  loadNeeds: function (loader, parentModel) {
    var needLoaders = [],
        that = this,
        needs = parentModel.get("needs");

    if(needs) {
      MoreObject.forEach(needs, function (name, options) {
        var need = that.normalizeNeed(name, options),
            needLoader = loader.queryRecord(need.type, parentModel.get(need.idKey));

        needLoader.then(function (model) {
          parentModel.set(need.name, model);
        });

        if(need.silent) {
          needLoader = needLoader.catch(function () {
            parentModel.set(need.name, null);
          });
        }

        if(!need.lazy) {
          needLoaders.push(needLoader);
        }
      });
    }

    if(needLoaders.length) {
      return Ember.RSVP.all(needLoaders);
    }
  },

});
