<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
Tez debugging tools webapp
=========

This is a post-hoc analysis tool for Apache Tez
which makes easier to run some tools with a web UI.

You can run it directly via maven...

```bash
#cd tez-tools/tez-tools-webapp #assuming that you're already here
mvn clean install -DskipTests spring-boot:run
```

...or build a docker image and run it (anytime later from your machine):

```bash
#cd tez-tools/tez-tools-webapp #assuming that you're already here
mvn clean install -DskipTests
docker build -t tez-tools-webapp .
docker run -p 8080:8080 tez-tools-webapp
```

Then navigate to: <http://localhost:8080>
