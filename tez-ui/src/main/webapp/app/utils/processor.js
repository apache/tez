/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import Ember from 'ember';

function getVibrantHSL(colorNum, totalColors) {
  if (totalColors < 1){
    totalColors = 1;
  }
  return {
    h: colorNum * (360 / totalColors) % 360,
    s: 100 - (colorNum % 2) * 30,
    l: 40
  };
}

export default Ember.Object.extend({

  processCount: 0,

  startTime: 0,
  endTime: 0,

  timeWindow: Ember.computed("startTime", "endTime", function () {
    return Math.max(0, this.get("endTime") - this.get("startTime"));
  }),

  createProcessColor: function (index, totalProcessCount) {
    return getVibrantHSL(index, totalProcessCount || this.get("processCount"));
  },

  timeToPositionPercent: function (time) {
    return ((time - this.get("startTime")) / this.get("timeWindow")) * 100;
  }

});
