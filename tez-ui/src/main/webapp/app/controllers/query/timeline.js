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
import TableController from '../table';
import ColumnDefinition from 'em-table/utils/column-definition';

var MoreObject = more.Object;

export default TableController.extend({

  columns: ColumnDefinition.make([{
    id: 'perfLogName',
    headerTitle: 'Raw Perf Log Name',
    contentPath: 'perfLogName',
  }, {
    id: 'perfLogValue',
    headerTitle: 'Value',
    contentPath: 'perfLogValue',
    cellDefinition: {
      type: 'duration'
    }
  }]),

  phaseTimes: Ember.computed("model", function () {
    var perf = this.get("model.perf");
    return {
      pre: perf.compile + perf.parse + perf.semanticAnalyze + perf.TezBuildDag,
      submission: perf.TezSubmitDag + perf.TezSubmitToRunningDag,
      run: perf.TezRunDag,
      post: perf.RemoveTempOrDuplicateFiles +
          perf.RemoveTempOrDuplicateFiles +
          perf.RenameOrMoveFiles
    };
  }),

  rows: Ember.computed("model.perf", function () {
    var perf = this.get("model.perf"),
        rows = [];

    if(perf) {
      MoreObject.forEach(perf, function (key, value) {
        rows.push(Ember.Object.create({
          perfLogName: key,
          perfLogValue: value
        }));
      });
    }

    return Ember.A(rows);
  })

});
