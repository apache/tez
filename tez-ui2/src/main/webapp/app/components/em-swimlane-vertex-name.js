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

const MAX_TEXT_LENGTH = 10;

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

  useEllipsis: Ember.computed("process.name", "progressText", function () {
    var name = this.get("process.name") || "",
        progressLength = this.get("progressText.length");
    progressLength = progressLength ? progressLength + 1 : 0;
    return  name.length + progressLength - 1 > MAX_TEXT_LENGTH;
  }),

  processName: Ember.computed("process.name", "progressText", function () {
    var name = this.get("process.name") || "",
        progressLength = this.get("progressText.length");
    progressLength = progressLength ? progressLength + 1 : 0;
    return name.substr(Math.max(name.length - MAX_TEXT_LENGTH - progressLength, 0));
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
