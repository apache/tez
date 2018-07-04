<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

# Tez-ui

The Tez UI is an ember based web-app that provides visualization of Tez applications running on the Apache Hadoop YARN framework.

For more information on Tez and the Tez UI - Check the [Tez homepage](http://tez.apache.org/ "Apache Tez Homepage").

## Configurations

* By default timeline is expected at localhost:8188 & RM at localhost:8088
* You can point the UI to custom locations by setting the environment variables in `src/main/webapp/config/configs.js`

## Development

All the following commands must be run inside `src/main/webapp`.

### Prerequisites

You will need the following things properly installed on your computer.

* Install [Yarn](https://yarnpkg.com) v0.21.3
* Install [Bower](http://bower.io/) v1.7.7
* Install all dependencies by running `yarn install` & `bower install`

### Running UI

* `yarn start`
* Visit your app at [http://localhost:4200](http://localhost:4200).

### Running Tests

* `yarn test`

### Building

* `yarn run build` (production)
* Files would be stored in "dist/"

### Adding new dependencies

**Warning: Do not edit the _package.json_ or _bower.json_ files manually. This could make them out-of-sync with the respective lock or shrinkwrap files.**

Tez UI has replaced NPM with Yarn package manager. And hence Yarn would be used to manage dependencies defined in package.json.

* Please use the Yarn and Bower command-line tools to add new dependencies. And the tool version must be same as those defined in Prerequisites section.
* Once any dependency is added:
  *  If it's in package.json. Make sure that the respective, and only those changes are reflected in yarn.lock file.
  *  If it's in bower.json. Make sure that the respective, and only those changes are reflected in bower-shrinkwrap.json file.
* Commands to add using CLI tools:
  * Yarn: yarn add [package-name]
  * Bower: bower install --save [package-name]

### Adding new routes (pages), controllers, components etc.

* Use ember-cli blueprint generator - [Ember CLI](http://ember-cli.com/extending/#generators-and-blueprints)
