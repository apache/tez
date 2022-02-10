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

import EmberObject from '@ember/object';
import { run } from '@ember/runloop';
import { module, test } from 'qunit';
import { settled } from '@ember/test-helpers';

import DataProcessor from '../../../utils/data-processor';
import ColumnDefinition from '../../../utils/column-definition';

module('Unit | Utility | data processor', function() {
  test('Class creation test', function(assert) {
    assert.ok(DataProcessor);
  });

  test('Instance default test', function(assert) {
    var processor;

    run(function () {
      processor = DataProcessor.create({
        tableDefinition: EmberObject.create(),
        startSearch: function () {
          // Test Search
        },
        startSort: function () {
          // Test Sort
        }
      });
    });

    assert.ok(processor);
    assert.false(processor.get('isSorting'));
    assert.false(processor.get('isSearching'));

    assert.ok(processor._searchObserver);
    assert.ok(processor._sortObserver);
    assert.ok(processor.startSearch);
    assert.ok(processor.startSort);
    assert.ok(processor.compareFunction);
  });

  test('compareFunction test', function(assert) {
    var processor;

    run(function () {
      processor = DataProcessor.create({
        tableDefinition: EmberObject.create(),
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

  test('startSearch test', async function(assert) {
    var processor;

    assert.expect(3);

    run(function () {
      processor = DataProcessor.create({
        tableDefinition: EmberObject.create({
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
        _sortedRows: [EmberObject.create({
          foo: "Foo1",
          bar: "Bar1"
        }), EmberObject.create({
          foo: "Foo12",
          bar: "Bar2"
        }), EmberObject.create({
          foo: "Foo3",
          bar: "Bar3"
        }), EmberObject.create({
          foo: "Foo4",
          bar: "Bar4"
        })],
      });
    });
    // startSearch adds a later to the runloop. Waiting until the runloop is emtpy (settled)
    // before asserting the results
    await settled();
    assert.equal(processor.get("_searchedRows.length"), 2);
    assert.equal(processor.get("_searchedRows.0.foo"), "Foo1");
    assert.equal(processor.get("_searchedRows.1.foo"), "Foo12");

  });
});
