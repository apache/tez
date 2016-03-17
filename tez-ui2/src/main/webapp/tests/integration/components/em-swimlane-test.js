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

import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('em-swimlane', 'Integration | Component | em swimlane', {
  integration: true
});

test('Basic creation test', function(assert) {
  var testName1 = "TestName1",
      testName2 = "TestName2";

  this.set("processes", [{
    name: testName1
  }, {
    name: testName2
  }]);

  this.render(hbs`{{em-swimlane processes=processes}}`);

  assert.equal(this.$().text().trim().indexOf(testName1), 0);
  assert.notEqual(this.$().text().trim().indexOf(testName2), -1);

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-swimlane processes=processes}}
      template block text
    {{/em-swimlane}}
  `);

  assert.equal(this.$().text().trim().indexOf(testName1), 0);
  assert.notEqual(this.$().text().trim().indexOf(testName2), -1);
});
