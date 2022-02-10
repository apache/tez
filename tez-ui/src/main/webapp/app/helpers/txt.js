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

import { helper as buildHelper } from '@ember/component/helper';
import { htmlSafe } from '@ember/template';
import formatters from '../utils/formatters';

const escape = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#x27;',
  '`': '&#x60;',
  '=': '&#x3D;',
};

const possible = /[&<>"'`=]/;
const badChars = /[&<>"'`=]/g;

function escapeChar(chr) {
  return escape[chr];
}

// Replacement for Ember.Handlebars.Utils.escapeExpression
export function escapeExpression(string) {
  if (typeof string !== 'string') {
    // don't escape SafeStrings, since they're already safe
    if (string && string.toHTML) {
      return string.toHTML();
    } else if (string === null || string === undefined) {
      return '';
    } else if (!string) {
      return String(string);
    }

    // Force a string conversion as this will be done by the append regardless and
    // the regex test will do this transparently behind the scenes, causing issues if
    // an object's to string has escaped characters in it.
    string = String(string);
  }

  if (!possible.test(string)) {
    return string;
  }
  return string.replace(badChars, escapeChar);
}

export function txt(value, hash) {
  var message,
      dataType = hash.type,
      formatter = hash.formatter,
      titleAttr = "";

  if(value) {
    value = value[0];
  }

  if(value instanceof Error) {
    message = value.message;
    titleAttr = `title="${value.message}" `;
  }
  else {
    try {
      if(value !== undefined && !formatter && dataType) {
        formatter = formatters[dataType];
      }

      if(formatter && value !== undefined && value !== null) {
        value = formatter(value, hash);
      }

      if(value === undefined || value === null) {
        message = 'Not Available!';
      }
      else {
        return htmlSafe(escapeExpression(value.toString()));
      }
    }
    catch(error) {
      message = "Invalid Data!";
      console.error(error);
    }
  }

  return htmlSafe(`<span ${titleAttr}class="txt-message"> ${message} </span>`);
}

export default buildHelper(txt);
