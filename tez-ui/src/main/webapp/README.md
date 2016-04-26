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
* You can point the UI to custom locations by setting the environment variables in `src/main/webapp/config/configs.env`

## Development

All the following commands must be run inside `src/main/webapp`.

### Prerequisites

You will need the following things properly installed on your computer.

* Install [Node.js](http://nodejs.org/) (with NPM)
* Install [Bower](http://bower.io/)
* Install all dependencies by running `npm install` & `bower install`

### Running UI

* `npm start`
* Visit your app at [http://localhost:4200](http://localhost:4200).

### Running Tests

* `npm test`

### Building

* `npm run build` (production)
* Files would be stored in "dist/"

### Adding new routes (pages), controllers, components etc.

* Use ember-cli blueprint generator - [Ember CLI](http://ember-cli.com/extending/#generators-and-blueprints)
