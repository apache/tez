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

const POM_FILE = "../../../pom.xml";
var fs = require('fs');

function fetchVersion() {
  try {
    var fileData = fs.readFileSync(POM_FILE, 'ascii');
    // Feel this is better than parsing the whole xml
    return fileData.substring(fileData.indexOf("<version>") + 9, fileData.indexOf("</version>"));
  }catch(e){}
}

module.exports = {
  version: fetchVersion()
};
