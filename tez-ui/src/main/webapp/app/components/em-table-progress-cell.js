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

import { getOwner } from '@ember/application';
import Component from '@ember/component';
import EmberObject, { computed } from '@ember/object';
import layout from '../templates/components/em-table-progress-cell';

export default Component.extend({
  layout: layout,

  content: null,

  message: computed("content", function () {
    var content = this.content;

    if(content === undefined || content === null) {
      return "Not Available!";
    }
    else if(isNaN(parseFloat(content))){
      return "Invalid Data!";
    }
  }),

  _definition: computed("definition", function () {
    var object = EmberObject.extend({
      valueMin: 0,
      valueMax: 1,
      striped: true,
      style: null
    }).create();
    // TODO}).create(getOwner(this).ownerInjection(), this.definition);
    return object;
  })
});
