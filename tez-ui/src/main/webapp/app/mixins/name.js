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

import { computed } from '@ember/object';
import Mixin from '@ember/object/mixin';

export default Mixin.create({

  name: computed({
    get() {
      if (this._name) {
        return this._name;
      }
      var name = this.toString();
      name = name.substr(0, name.indexOf("::"));
      name = name.substr(name.indexOf(":") + 1);
      return name;
    },

    set(key, value) {
      return this._name = value;
    }
  })
});
