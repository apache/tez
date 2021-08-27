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
import { module, test } from 'qunit';
import ColumnDefinition from '../../../utils/column-definition';

module('Unit | Utility | column definition', function() {
  test('Class creation test', function(assert) {
    assert.ok(ColumnDefinition);

    assert.ok(ColumnDefinition.make);
  });

  test('make - Instance creation test', function(assert) {

    var definition = ColumnDefinition.make({
      id: "testId"
    });
    var definitions = ColumnDefinition.make([{
      id: "testId 1"
    },{
      id: "testId 2"
    }]);

    // Single
    assert.ok(definition);

    // Multiple
    assert.ok(definitions);
    assert.ok(Array.isArray(definitions));
    assert.equal(definitions.length, 2);
  });

  test('make - Instance creation failure test', function(assert) {
    assert.throws(function () {
      ColumnDefinition.make({});
    });
  });


  test('Instance test', function(assert) {
    var definition = ColumnDefinition.make({
      id: "testId",
      contentPath: "a.b"
    });
    var data = EmberObject.create({
      a: {
        b: 42
      }
    });

    assert.ok(definition.getCellContent);
    assert.ok(definition.getSearchValue);
    assert.ok(definition.getSortValue);

    assert.equal(definition.id, "testId");
    assert.equal(definition.headerTitle, "Not Available!");
    assert.equal(definition.minWidth, "150px");
    assert.equal(definition.contentPath, "a.b");

    assert.equal(definition.getCellContent(data), 42);
    assert.equal(definition.getSearchValue(data), 42);
    assert.equal(definition.getSortValue(data), 42);
  });
});
