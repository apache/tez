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

import { setupRenderingTest } from 'ember-qunit';
import { module, test } from 'qunit';
import { findAll, render } from '@ember/test-helpers';
import { hbs } from 'ember-cli-htmlbars';

module('Integration | Component | em table tasks log link cell', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic render test', async function(assert) {
    await render(hbs`<EmTableTasksLogLinkCell/>`);

    assert.dom(this.element).hasText('Not Available!');

    // Template block usage:" + EOL +
    await render(hbs`
      <EmTableTasksLogLinkCell>
        template block text
      </EmTableTasksLogLinkCell>
    `);

    assert.equal(this.element.textContent.trim(), 'Not Available!');
  });

  test('Test with content', async function(assert) {
    let attemptID = "attempt_1";

    this.set("content", attemptID);
    await render(hbs`<EmTableTasksLogLinkCell @content={{this.content}}/>`);

    let tags = findAll("a");
    assert.equal(tags.length, 2);
    assert.dom(tags[0]).hasText('View');
    assert.dom(tags[1]).hasText('Download');
  });
});
