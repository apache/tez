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

  getDisplayedPerfValues: function (perfHash) {
    return [[
      perfHash["compile"] || 0,
      perfHash["parse"] || 0,
      perfHash["semanticAnalyze"] || 0,
      perfHash["TezBuildDag"] || 0,
    ], [
      perfHash["TezSubmitDag"] || 0,
      perfHash["TezSubmitToRunningDag"] || 0,
    ], [
      perfHash["TezRunDag"] || 0,
    ], [
      perfHash["PostATSHook"] || 0,
      perfHash["RemoveTempOrDuplicateFiles"] || 0,
      perfHash["RenameOrMoveFiles"] || 0,
    ]];
  },

  alignBars: function (bars, widthFactors) {
    var totalValue = widthFactors.reduce((a, b) => a + b, 0);
    bars.each(function (index, bar) {
      var width = (widthFactors[index] / totalValue) * 100;
      Ember.$(bar).css({
        width: `${width}%`
      });
    });
  },

  didInsertElement: Ember.observer("perf", function () {
    var perfs = this.getDisplayedPerfValues(this.get("perf"));

    this.alignBars(this.$().find(".sub-groups").find(".bar"), [].concat.apply([], perfs));

    this.alignBars(this.$().find(".groups").find(".bar"), perfs.map(function (subPerfs) {
      return subPerfs.reduce((a, b) => a + b, 0);
    }));
  })

});
