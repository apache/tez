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

import TimelineSerializer from './timeline';

export default TimelineSerializer.extend({
  maps: {
    dagPlan: 'otherinfo.dagPlan',
    callerData: 'callerData',
  },

  normalizeResourceHash: function (resourceHash) {
    var data = resourceHash.data,
        callerData = {},
        dagInfo = Ember.get(data, "otherinfo.dagPlan.dagInfo"), // New style, from TEZ-2851
        dagContext = Ember.get(data, "otherinfo.dagPlan.dagContext"); // Old style

    if(dagContext) {
      callerData.callerContext = Ember.String.classify((Ember.get(dagContext, "context")||"").toLowerCase());
      callerData.callerDescription = Ember.get(dagContext, "description");
      callerData.callerType = Ember.get(dagContext, "callerType") || Ember.get(data, "otherinfo.callerType");
    }
    else if(dagInfo) {
      let infoObj = {};
      try{
        infoObj = JSON.parse(dagInfo);
      }catch(e){
        infoObj = dagInfo;
      }

      callerData.callerContext = Ember.get(infoObj, "context") || Ember.get(data, "otherinfo.callerContext");
      callerData.callerDescription = Ember.get(infoObj, "description") || Ember.get(dagInfo, "blob") || dagInfo;
    }

    data.callerData = callerData;

    return resourceHash;
  },

  extractAttributes: function (modelClass, resourceHash) {
    return this._super(modelClass, this.normalizeResourceHash(resourceHash));
  },
});