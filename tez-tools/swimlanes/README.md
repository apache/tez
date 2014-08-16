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

Swimlanes
=========

This is a performance and post-hoc analysis tool for Apache Tez which uses the log lines
printed during execution to draw a diagram which represents each container allocated
with a horizontal lane, where each task is represented as a section of the lane. 

Currently, the tool only works for successfully completed DAGs and does not have much way
of representing failures or data dependency in the diagram.

The data output format is SVG, which also includes clickable links back to the history logs 
of each task for further debugging, after a slow task has been located. 

To use the tool, run

`./yarn-swimlanes.sh application_..._..`

This will generate an svg named the same as the application ID.

For more information on using this tool visit - the [tez wiki](https://cwiki.apache.org/confluence/display/TEZ/Using+tez-tools+to+analyze+jobs "Using tez-tools to analyze jobs").
