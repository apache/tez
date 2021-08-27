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
import { render, findAll, find } from '@ember/test-helpers';
import { hbs } from 'ember-cli-htmlbars';

module('Integration | Component | zip download modal', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {
    var testID = "dag_a",
        expectedMessage = "Downloading data for dag: " + testID;

    this.set("content", {
      dag: {
        entityID: testID
      }
    });

    await render(hbs`{{zip-download-modal content=content}}`);
    assert.equal(find(".message").textContent.trim().indexOf(expectedMessage), 0);

    // Template block usage:" + EOL +
    await render(hbs`
      {{#zip-download-modal content=content}}
        template block text
      {{/zip-download-modal}}
    `);
    assert.equal(find(".message").textContent.trim().indexOf(expectedMessage), 0);
  });

  test('progress test', async function(assert) {
    this.set("content", {
      downloader: {
        percent: 0.5
      }
    });

    await render(hbs`{{zip-download-modal content=content}}`);
    let text = find(".message").textContent.trim();
    assert.equal(text.substr(-3), "50%");

    assert.equal(findAll(".btn").length, 1);
    assert.equal(findAll(".btn-primary").length, 0);
  });

  test('failed test', async function(assert) {
    var expectedMessage = "Error downloading data!";

    this.set("content", {
      downloader: {
        failed: true
      }
    });

    await render(hbs`{{zip-download-modal content=content}}`);
    assert.equal(find(".message").textContent.trim().indexOf(expectedMessage), 0);

    assert.equal(findAll(".btn").length, 1);
    assert.equal(findAll(".btn-primary").length, 1);
  });

  test('partial test', async function(assert) {
    var expectedMessage = "Data downloaded might be incomplete. Please check the zip!";

    this.set("content", {
      downloader: {
        succeeded: true,
        partial: true
      }
    });

    await render(hbs`{{zip-download-modal content=content}}`);
    assert.equal(find(".message").textContent.trim().indexOf(expectedMessage), 0);

    assert.equal(findAll(".btn").length, 1);
    assert.equal(findAll(".btn-primary").length, 1);
  });
});
