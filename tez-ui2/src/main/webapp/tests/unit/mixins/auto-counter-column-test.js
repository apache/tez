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
import AutoCounterColumnMixin from '../../../mixins/auto-counter-column';
import { module, test } from 'qunit';

module('Unit | Mixin | auto counter column');

test('Basic creation test', function(assert) {
  let AutoCounterColumnObject = Ember.Object.extend(AutoCounterColumnMixin);
  let subject = AutoCounterColumnObject.create();

  assert.ok(subject);
  assert.ok(subject.columnSelectorMessage);
  assert.ok(subject.getCounterColumns);
});

test('getCounterColumns test', function(assert) {
  let TestParent = Ember.Object.extend({
    getCounterColumns: function () { return []; }
  });

  let AutoCounterColumnObject = TestParent.extend(AutoCounterColumnMixin);
  let subject = AutoCounterColumnObject.create({
    model: [{
      counterGroupsHash: {
        gp1: {
          c11: "v11",
          c12: "v12"
        }
      }
    }, {
      counterGroupsHash: {
        gp2: {
          c21: "v21",
          c22: "v22"
        },
        gp3: {
          c31: "v31",
          c32: "v32"
        }
      }
    }]
  });

  let columns = subject.getCounterColumns();
  assert.equal(columns.length, 6);
  assert.equal(columns[0].counterGroupName, "gp1");
  assert.equal(columns[0].counterName, "c11");
  assert.equal(columns[1].counterGroupName, "gp1");
  assert.equal(columns[1].counterName, "c12");

  assert.equal(columns[2].counterGroupName, "gp2");
  assert.equal(columns[2].counterName, "c21");
  assert.equal(columns[3].counterGroupName, "gp2");
  assert.equal(columns[3].counterName, "c22");

  assert.equal(columns[4].counterGroupName, "gp3");
  assert.equal(columns[4].counterName, "c31");
  assert.equal(columns[5].counterGroupName, "gp3");
  assert.equal(columns[5].counterName, "c32");
});
