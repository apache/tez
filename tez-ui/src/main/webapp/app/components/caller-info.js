/*global CodeMirror*/
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

// Must be convert into an ember addon

import Ember from 'ember';

export default Ember.Component.extend({

  type: null,
  info: null,

  codeMirror: null,

  classNames: ['caller-info'],

  mode: Ember.computed("type", function () {
    switch(this.get("type")) {
      case 'Hive':
        return 'text/x-hive';
      case 'Pig':
        return 'text/x-pig';
      default:
        return 'text/x-sql';
    }
  }),

  _init:  Ember.on('didInsertElement', function() {
    var element  = Ember.$(this.get('element')).find('textarea')[0],
        codeMirror = CodeMirror.fromTextArea(element, {
          theme: 'default',
          indentUnit: 2,
          smartIndent: true,
          tabSize: 4,
          electricChars: true,
          lineWrapping: true,
          lineNumbers: true,
          readOnly: true,
          autofocus: false,
          dragDrop: false,
        });

    this.set('codeMirror', codeMirror);

    this._modeChanged();
    this._infoChanged();
  }),

  _modeChanged: Ember.observer("mode", function() {
    this.get('codeMirror').setOption("mode", this.get("mode"));
  }),

  _infoChanged: Ember.observer("info", function() {
    var codeMirror = this.get('codeMirror'),
        info = this.get('info') || '';

    if (this.get('codeMirror').getValue() !== info) {
      codeMirror.setValue(info);
    }
  })

});
