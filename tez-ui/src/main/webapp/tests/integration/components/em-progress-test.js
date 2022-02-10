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
import { find, render } from '@ember/test-helpers';
import { hbs } from 'ember-cli-htmlbars';

module('Integration | Component | em progress', function(hooks) {
  setupRenderingTest(hooks);

  test('It renders', async function(assert) {

    await render(hbs`<EmProgress/>`);

    assert.dom(this.element).hasText('0%');

    await render(hbs`<EmProgress></EmProgress>`);
    assert.dom(this.element).hasText('0%');
  });

  test('With a specific value', async function(assert) {
    await render(hbs`<EmProgress @value={{0.5}}/>`);
    assert.dom(this.element).hasText('50%');
  });

  test('Custom valueMin & valueMax', async function(assert) {
    await render(hbs`<EmProgress @value={{15}} @valueMin={{10}} @valueMax={{20}}/>`);
    assert.dom(this.element).hasText('50%');
    assert.notOk(find('.striped'), "Striped class added");
  });

  test('Check for stripes & animation while in progress', async function(assert) {
    await render(hbs`<EmProgress @value={{0.5}} @striped={{true}}/>`);

    assert.dom(this.element).hasText('50%');
    let container = find('.em-progress-container');
    let progressBar = find('.progress-bar');
    assert.ok(container, "em-progress-container class should be found");
    assert.ok(progressBar, "progress-bar class should be found");
    assert.dom(container).hasClass('striped', "Striped class not found");
    assert.dom(container).hasClass('animated', "Animated class not found");
    assert.dom(progressBar).hasClass('progress-bar-striped', "progress-bar-striped class not found");
    assert.dom(progressBar).hasClass('active', "active class not found");
  });

  test('Check for stripes & animation while starting', async function(assert) {
    await render(hbs`<EmProgress @value={{0}} @striped={{true}}/>`);

    assert.dom(this.element).hasText('0%');
    let container = find('.em-progress-container');
    let progressBar = find('.progress-bar');
    assert.ok(container, "em-progress-container class should be found");
    assert.ok(progressBar, "progress-bar class should be found");
    assert.dom(container).hasClass('striped', "Striped class not found");
    assert.dom(container).doesNotHaveClass('animated', "Animated class should not found");
    assert.dom(progressBar).hasClass('progress-bar-striped', "progress-bar-striped class not found");
    assert.dom(progressBar).doesNotHaveClass('active', "active class should not be found");
  });

  test('Check for stripes & animation on completion', async function(assert) {
    await render(hbs`<EmProgress @value={{1}} @striped={{true}}/>`);

    assert.dom(this.element).hasText('100%');
    let container = find('.em-progress-container');
    let progressBar = find('.progress-bar');
    assert.ok(container, "em-progress-container class should be found");
    assert.ok(progressBar, "progress-bar class should be found");
    assert.dom(container).hasClass('striped', "Striped class not found");
    assert.dom(container).doesNotHaveClass('animated', "Animated class should not found");
    assert.dom(progressBar).hasClass('progress-bar-striped', "progress-bar-striped class not found");
    assert.dom(progressBar).doesNotHaveClass('active', "active class should not be found");
    assert.equal(this.element.textContent.trim(), '100%');
  });
});
