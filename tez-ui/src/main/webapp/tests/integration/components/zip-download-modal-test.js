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

moduleForComponent('zip-download-modal', 'Integration | Component | zip download modal', {
  integration: true
});

test('Basic creation test', function(assert) {
  var testID = "dag_a",
      expectedMessage = "Downloading data for dag: " + testID;

  this.set("content", {
    dag: {
      entityID: testID
    }
  });

  this.render(hbs`{{zip-download-modal content=content}}`);
  assert.equal(this.$(".message").text().trim().indexOf(expectedMessage), 0);

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#zip-download-modal content=content}}
      template block text
    {{/zip-download-modal}}
  `);
  assert.equal(this.$(".message").text().trim().indexOf(expectedMessage), 0);
});

test('progress test', function(assert) {
  this.set("content", {
    downloader: {
      percent: 0.5
    }
  });

  this.render(hbs`{{zip-download-modal content=content}}`);
  let text = this.$(".message").text().trim();
  assert.equal(text.substr(-3), "50%");

  assert.equal(this.$(".btn").length, 1);
  assert.equal(this.$(".btn-primary").length, 0);
});

test('failed test', function(assert) {
  var expectedMessage = "Error downloading data!";

  this.set("content", {
    downloader: {
      failed: true
    }
  });

  this.render(hbs`{{zip-download-modal content=content}}`);
  assert.equal(this.$(".message").text().trim().indexOf(expectedMessage), 0);

  assert.equal(this.$(".btn").length, 1);
  assert.equal(this.$(".btn-primary").length, 1);
});

test('partial test', function(assert) {
  var expectedMessage = "Data downloaded might be incomplete. Please check the zip!";

  this.set("content", {
    downloader: {
      succeeded: true,
      partial: true
    }
  });

  this.render(hbs`{{zip-download-modal content=content}}`);
  assert.equal(this.$(".message").text().trim().indexOf(expectedMessage), 0);

  assert.equal(this.$(".btn").length, 1);
  assert.equal(this.$(".btn-primary").length, 1);
});