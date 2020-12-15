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

Tez log splitter
=========

This is a post-hoc analysis tool for Apache Tez which splits
an aggregated yarn log file to separate files into a hierarchical folder structure.

```
.
├── vc0525.your.domain.com_8041
│   └── container_e10_1575565459633_0004_01_000001
│       ├── container-localizer-syslog
│       ├── dag_1575565459633_0004_1-tez-dag.pb.txt
│       ├── dag_1575565459633_0004_1.dot
│       ├── prelaunch.err
│       ├── prelaunch.out
│       ├── stderr
│       ├── stdout
│       ├── syslog
│       ├── syslog_dag_1575565459633_0004_1
│       └── syslog_dag_1575565459633_0004_1_post
├── vc0526.your.domain.com_8041
│   └── container_e10_1575565459633_0004_01_000004
│       ├── container-localizer-syslog
│       ├── prelaunch.err
│       ├── prelaunch.out
│       ├── stderr
│       ├── stdout
│       ├── syslog
│       └── syslog_attempt_1575565459633_0004_1_00_000000_2
├── vc0528.your.domain.com_8041
│   └── container_e10_1575565459633_0004_01_000002
│       ├── container-localizer-syslog
│       ├── prelaunch.err
│       ├── prelaunch.out
│       ├── stderr
│       ├── stdout
│       ├── syslog
│       └── syslog_attempt_1575565459633_0004_1_00_000000_0
├── vc0529.your.domain.com_8041
│   └── container_e10_1575565459633_0004_01_000005
│       ├── container-localizer-syslog
│       ├── prelaunch.err
│       ├── prelaunch.out
│       ├── stderr
│       ├── stdout
│       ├── syslog
│       └── syslog_attempt_1575565459633_0004_1_00_000000_3
└── vc0536.your.domain.com_8041
    └── container_e10_1575565459633_0004_01_000003
        ├── container-localizer-syslog
        ├── prelaunch.err
        ├── prelaunch.out
        ├── stderr
        ├── stdout
        ├── syslog
        └── syslog_attempt_1575565459633_0004_1_00_000000_1
```

To use the tool, run e.g.

`tez-log-splitter.sh application_1576254620247_0010`  (app log is fetched from yarn)  
`tez-log-splitter.sh ~/path/to/application_1576254620247_0010.log`  (...when app log is already on your computer)  
`tez-log-splitter.sh ~/path/to/application_1576254620247_0010.log.gz`  (...when app log is already on your computer in gz)
