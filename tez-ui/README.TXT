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

Configurations
--------------
  For the UI to work as expected with all the required data, the following configuration options
should be set.

In tez-site.xml
  * tez.runtime.convert.user-payload.to.history-text
      Should be enabled to get the configuration options. If enabled, the config options are set
    as userpayload per input/output.

In yarn-site.xml
  * yarn.timeline-service.http-cross-origin.enabled
      Enable CORS in timeline.
  * yarn.resourcemanager.system-metrics-publisher.enabled
      Enable generic history service in timeline server
  * yarn.timeline-service.enabled
      Enabled the timeline server for logging details
  * yarn.timeline-service.webapp.address
      Value must be the IP:PORT on which timeline server is running

In configs.js
  * Both timeline and RM base URLs can be customized in app/scripts/configs.js. By default their
    values are http://localhost:8188 and http://localhost:8088 respectively.
  * More counter columns can be added to all tables through the file. More information regarding
    the same can be found in configs.js.

Building
---------
  On building tez-ui two files will be created in the target directory, a tarball and a war file.
Both packages the tez-ui distribution files, and can be found inside tez-ui-x.x.x-SNAPSHOT directory.
To fire a test less build in the tez-ui run 'mvn clean package -DskipTests'. Tez ui build is part of
tez build, and hence building tez would build tez-ui also. Refer BUILDING.txt in tez for more info.

Dev setup instructions
----------------------
Install the development dependencies nodejs, npm, grunt and bower
from webapp directory run the following commands
  * npm install
  * bower install

For development run 'grunt serve'. This runs a dev server on port 9001.
navigate to http://localhost:9001 if a browser does not open automatically.
Any changes made will be live-reloaded on the browser.

