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

import { module, test } from 'qunit';
import { settled } from '@ember/test-helpers';
import TableDefinition from '../../../utils/table-definition';

module('Unit | Utility | table definition', function() {
  test('Class creation test', function(assert) {
    assert.ok(TableDefinition);
  });

  test('Default instance test', function(assert) {
    var definition = TableDefinition.create();

    assert.ok(definition);

    assert.equal(definition.pageNum, 1);
    assert.equal(definition.rowCount, 10);
    assert.equal(definition.minRowsForFooter, 25);
  });

  test('Page-num reset test', async function(assert) {
    var definition = TableDefinition.create();

    await settled();
    assert.equal(definition.pageNum, 1);

    definition.set("pageNum", 5);
    await settled();
    assert.equal(definition.pageNum, 5);

    definition.set("searchText", "x");
    await settled();
    assert.equal(definition.pageNum, 1);

    definition.set("pageNum", 5);
    definition.set("rowCount", 5);
    await settled();
    assert.equal(definition.pageNum, 1);
  });
});
