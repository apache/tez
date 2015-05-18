/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

App.CodeMirrorComponent = Em.Component.extend({
  tagName: 'textarea',

  value: null,

  mode: 'text/x-hive',
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

  _init:  Em.on('didInsertElement', function() {
    var codeMirror = CodeMirror.fromTextArea(this.get('element'));

    this.set('codeMirror', codeMirror);

    // Set up bindings for CodeMirror options.
    this._bindCodeMirrorOption('mode');
    this._bindCodeMirrorOption('theme');
    this._bindCodeMirrorOption('indentUnit');
    this._bindCodeMirrorOption('smartIndent');
    this._bindCodeMirrorOption('tabSize');
    this._bindCodeMirrorOption('electricChars');
    this._bindCodeMirrorOption('lineWrapping');
    this._bindCodeMirrorOption('lineNumbers');
    this._bindCodeMirrorOption('readOnly');
    this._bindCodeMirrorOption('autofocus');
    this._bindCodeMirrorOption('dragDrop');

    this._bindProperty('value', this, '_valueDidChange');
    this._valueDidChange();

    this.on('becameVisible', codeMirror, 'refresh');
  }),

  _bindCodeMirrorOption: function(key) {
    this._bindProperty(key, this, '_optionDidChange');

    // Set the initial option synchronously.
    this._optionDidChange(this, key);
  },

  _bindProperty: function(key, target, method) {
    this.addObserver(key, target, method);

    this.on('willDestroyElement', function() {
      this.removeObserver(key, target, method);
    })
  },

  _optionDidChange: function(sender, key) {
    this.get('codeMirror').setOption(key, this.get(key));
  },

  _valueDidChange: function() {
    var codeMirror = this.get('codeMirror'),
        value = this.get('value') || '';

    if (this.get('codeMirror').getValue() != value) {
      codeMirror.setValue(value);
    }
  }
});
