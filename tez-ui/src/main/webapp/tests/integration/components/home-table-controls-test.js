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
import { render } from '@ember/test-helpers';
import { setupRenderingTest } from 'ember-qunit';
import { hbs } from 'ember-cli-htmlbars';
import EmberObject from '@ember/object';

module('Integration | Component | home table controls', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {
    await render(hbs`<HomeTableControls/>`);

    assert.dom(this.element).hasText('Load Counters');
    assert.dom('button').hasClass("no-visible");

    // Template block usage:" + EOL +
    await render(hbs`
      <HomeTableControls>
        template block text
      </HomeTableControls>
    `);

    assert.dom(this.element).hasText('Load Counters');
  });

  test('countersLoaded test', async function(assert) {
    this.set("dataProcessor", {
      processedRows: [EmberObject.create({
        counterGroupsHash: {
          counter: {}
        }
      }), EmberObject.create({
        counterGroupsHash: {
          counter: {}
        }
      })]
    });
    await render(hbs`<HomeTableControls @dataProcessor={{this.dataProcessor}}/>`);
    assert.dom('button').hasClass("no-visible");

    this.set("dataProcessor", {
      processedRows: [EmberObject.create({
        counterGroupsHash: {}
      }), EmberObject.create({
        counterGroupsHash: {
          counter: {}
        }
      })]
    });
    await render(hbs`<HomeTableControls @dataProcessor={{this.dataProcessor}}/>`);
    assert.dom('button').doesNotHaveClass("no-visible");

    this.set("dataProcessor", {
      processedRows: [EmberObject.create({
        counterGroupsHash: {}
      }), EmberObject.create({
        counterGroupsHash: {}
      })]
    });
    await render(hbs`<HomeTableControls @dataProcessor={{this.dataProcessor}}/>`);
    assert.dom('button').doesNotHaveClass("no-visible");
  });
});
