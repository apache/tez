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

import Ember from 'ember';

moduleForComponent('home-table-controls', 'Integration | Component | home table controls', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.render(hbs`{{home-table-controls}}`);

  assert.equal(this.$().text().trim(), 'Load Counters');
  assert.equal(this.$().find("button").attr("class").split(" ").indexOf("no-visible"), 2);

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#home-table-controls}}
      template block text
    {{/home-table-controls}}
  `);

  assert.equal(this.$().text().trim(), 'Load Counters');
});

test('countersLoaded test', function(assert) {
  this.set("dataProcessor", {
    processedRows: [Ember.Object.create({
      counterGroupsHash: {
        counter: {}
      }
    }), Ember.Object.create({
      counterGroupsHash: {
        counter: {}
      }
    })]
  });
  this.render(hbs`{{home-table-controls dataProcessor=dataProcessor}}`);
  assert.equal(this.$().find("button").attr("class").split(" ").indexOf("no-visible"), 2);

  this.set("dataProcessor", {
    processedRows: [Ember.Object.create({
      counterGroupsHash: {}
    }), Ember.Object.create({
      counterGroupsHash: {
        counter: {}
      }
    })]
  });
  this.render(hbs`{{home-table-controls dataProcessor=dataProcessor}}`);
  assert.equal(this.$().find("button").attr("class").split(" ").indexOf("no-visible"), 2);

  this.set("dataProcessor", {
    processedRows: [Ember.Object.create({
      counterGroupsHash: {}
    }), Ember.Object.create({
      counterGroupsHash: {}
    })]
  });
  this.render(hbs`{{home-table-controls dataProcessor=dataProcessor}}`);
  assert.equal(this.$().find("button").attr("class").split(" ").indexOf("no-visible"), -1);
});
