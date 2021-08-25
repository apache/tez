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

moduleForComponent('em-table-facet-panel', 'Integration | Component | em table facet panel', {
  integration: true
});

test('Basic renders', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });" + EOL + EOL +

  this.render(hbs`{{em-table-facet-panel}}`);

  assert.equal(this.$().text().replace(/\n|\r\n|\r| /g, '').trim(), 'NotAvailable!');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-table-facet-panel}}
      template block text
    {{/em-table-facet-panel}}
  `);

  assert.equal(this.$().text().replace(/\n|\r\n|\r| /g, '').trim(), 'NotAvailable!');
});
