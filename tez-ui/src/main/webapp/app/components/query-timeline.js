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
  classNames: ['query-timeline'],

  perf: null,

  normalizedPerf: Ember.computed("perf", function () {
    var perf = this.get("perf") || {};

    // Create a copy of perf with default values
    perf = Ember.$.extend({
      compile: 0,
      parse: 0,
      TezBuildDag: 0,

      TezSubmitDag: 0,
      TezSubmitToRunningDag: 0,

      TezRunDag: 0,

      PostATSHook: 0,
      RemoveTempOrDuplicateFiles: 0,
      RenameOrMoveFiles: 0
    }, perf);

    perf.groupTotal = {
      pre: perf.compile + perf.parse + perf.TezBuildDag,
      submit: perf.TezSubmitDag + perf.TezSubmitToRunningDag,
      running: perf.TezRunDag,
      post: perf.PostATSHook + perf.RemoveTempOrDuplicateFiles + perf.RenameOrMoveFiles,
    };

    perf.total = perf.groupTotal.pre +
        perf.groupTotal.submit +
        perf.groupTotal.running +
        perf.groupTotal.post;

    return perf;
  }),

  alignBars: function (bars, perf) {
    bars.each(function (index, bar) {
      var width;

      bar = Ember.$(bar);
      width = (Ember.get(perf, bar.attr("data")) / perf.total) * 100;

      bar.css({
        width: `${width}%`
      });
    });
  },

  didInsertElement: Ember.observer("normalizePerf", function () {
    var perf = this.get("normalizedPerf");

    this.alignBars(this.$().find(".sub-groups").find(".bar"), perf);
    this.alignBars(this.$().find(".groups").find(".bar"), perf);
  })
});
