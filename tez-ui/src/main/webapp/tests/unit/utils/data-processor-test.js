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

import DataProcessor from '../../../utils/data-processor';
import ColumnDefinition from '../../../utils/column-definition';
import { module, test } from 'qunit';

module('Unit | Utility | data processor');

test('Class creation test', function(assert) {
  assert.ok(DataProcessor);
});

test('Instance default test', function(assert) {
  var processor;

  Ember.run(function () {
    processor = DataProcessor.create({
      tableDefinition: Ember.Object.create(),
      startSearch: function () {
        // Test Search
      },
      startSort: function () {
        // Test Sort
      }
    });
  });

  assert.ok(processor);
  assert.equal(processor.get('isSorting'), false);
  assert.equal(processor.get('isSearching'), false);

  assert.ok(processor._searchObserver);
  assert.ok(processor._sortObserver);
  assert.ok(processor.startSearch);
  assert.ok(processor.startSort);
  assert.ok(processor.compareFunction);
  assert.ok(processor.totalPages);
  assert.ok(processor.processedRows);
});

test('compareFunction test', function(assert) {
  var processor;

  Ember.run(function () {
    processor = DataProcessor.create({
      tableDefinition: Ember.Object.create(),
      startSearch: function () {},
      startSort: function () {}
    });
  });

  assert.equal(processor.compareFunction(1, 1), 0);
  assert.equal(processor.compareFunction(1, 2), -1);
  assert.equal(processor.compareFunction(2, 1), 1);

  assert.equal(processor.compareFunction("a", "a"), 0);
  assert.equal(processor.compareFunction("a", "b"), -1);
  assert.equal(processor.compareFunction("b", "a"), 1);

  assert.equal(processor.compareFunction(null, null), -1);
  assert.equal(processor.compareFunction(1, null), 1);
  assert.equal(processor.compareFunction(null, 2), -1);
  assert.equal(processor.compareFunction("a", null), 1);
  assert.equal(processor.compareFunction(null, "b"), -1);

  assert.equal(processor.compareFunction(undefined, undefined), -1);
  assert.equal(processor.compareFunction(1, undefined), 1);
  assert.equal(processor.compareFunction(undefined, 2), -1);
  assert.equal(processor.compareFunction("a", undefined), 1);
  assert.equal(processor.compareFunction(undefined, "b"), -1);
});

test('startSearch test', function(assert) {
  var processor,
      runLater = Ember.run.later;

  assert.expect(3);

  Ember.run.later = function (callback) {
    callback();
    assert.equal(processor.get("_searchedRows.length"), 2);
    assert.equal(processor.get("_searchedRows.0.foo"), "Foo1");
    assert.equal(processor.get("_searchedRows.1.foo"), "Foo12");

    Ember.run.later = runLater; // Reset
  };

  Ember.run(function () {
    processor = DataProcessor.create({
      tableDefinition: Ember.Object.create({
        searchText: "foo1",
        columns: [ColumnDefinition.create({
          id: "foo",
          contentPath: 'foo'
        }), ColumnDefinition.create({
          id: "bar",
          contentPath: 'bar'
        })]
      }),
      startSort: function () {
        // Test Sort
      },
      _sortedRows: [Ember.Object.create({
        foo: "Foo1",
        bar: "Bar1"
      }), Ember.Object.create({
        foo: "Foo12",
        bar: "Bar2"
      }), Ember.Object.create({
        foo: "Foo3",
        bar: "Bar3"
      }), Ember.Object.create({
        foo: "Foo4",
        bar: "Bar4"
      })],
    });
  });

});
