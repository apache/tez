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

import Component from '@ember/component';
import { computed } from '@ember/object';
import { on } from '@ember/object/evented';
import { scheduleOnce } from '@ember/runloop';
import { lineNumbers } from "@codemirror/gutter"
import { defaultHighlightStyle } from "@codemirror/highlight"
import { pig } from "@codemirror/legacy-modes/mode/pig"
import { hive } from "@codemirror/legacy-modes/mode/sql"
import { EditorState } from "@codemirror/state"
import { StreamLanguage } from "@codemirror/stream-parser"
import { EditorView } from "@codemirror/view"

export default Component.extend({

  type: null,
  info: null,

  title: null,

  codeMirror: null,

  classNames: ['caller-info'],

  mode: computed("type", function () {
    switch(this.type) {
      case 'Hive':
        return hive;
      case 'Pig':
        return pig;
      default:
        return null;
    }
  }),

  _init:  on('didInsertElement', function() {
    scheduleOnce('afterRender', this, function() {
      var element  = this.element.querySelector('textarea'),
          extensions = [
            defaultHighlightStyle.fallback,
            EditorView.editable.of(false),
            EditorView.lineWrapping,
            lineNumbers(),
            EditorState.readOnly.of(true),
          ];
      if (this.mode) {
        // only add parser for know types of pig and hive
        extensions.push(StreamLanguage.define(this.mode));
      }
      var codeMirror = this.editorFromTextArea(element, extensions);

      this.set('codeMirror', codeMirror);
    });
  }),

  editorFromTextArea: function(textarea, extensions) {
    let view = new EditorView({
      state: EditorState.create({doc: this.info, extensions})
    })
    textarea.parentNode.insertBefore(view.dom, textarea)
    textarea.style.display = "none"
    if (textarea.form) textarea.form.addEventListener("submit", () => {
      textarea.value = view.state.doc.toString()
    })
    return view
  },
});
