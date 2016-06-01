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

export default Ember.Component.extend({

  process: null,

  classNames: ["em-swimlane-vertex-name"],

  sendMouseAction: function (name, mouseEvent) {
    this.sendAction(name, "process-name", this.get("process"), {
      mouseEvent: mouseEvent,
    });
  },

  progressText: Ember.computed("process.vertex.finalStatus", "process.vertex.progress", function () {
    if(this.get("process.vertex.finalStatus") === "RUNNING") {
      let progress = this.get("process.vertex.progress");
      if(!isNaN(progress)) {
        let percent = parseInt(progress * 100);
        return `${percent}%`;
      }
    }
  }),

  mouseEnter: function (mouseEvent) {
    this.sendMouseAction("showTooltip", mouseEvent);
  },

  mouseLeave: function (mouseEvent) {
    this.sendMouseAction("hideTooltip", mouseEvent);
  },

  mouseUp: function (mouseEvent) {
    this.sendMouseAction("click", mouseEvent);
  }

});
