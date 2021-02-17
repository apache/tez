// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

pipeline {

    agent {
        label 'Hadoop'
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '5'))
        timeout (time: 20, unit: 'HOURS')
        timestamps()
        checkoutToSubdirectory('src')
    }

    environment {
        SOURCEDIR = 'src'
        // will also need to change notification section below
        PATCHDIR = 'out'
        DOCKERFILE = "${SOURCEDIR}/build-tools/docker/Dockerfile"
        YETUS='yetus'
        // Branch or tag name.  Yetus release tags are 'rel/X.Y.Z'
        YETUS_VERSION='6ab19e71eaf3234863424c6f684b34c1d3dcc0ce'
    }

    stages {
        stage ('install yetus') {
            steps {
                dir("${WORKSPACE}/${YETUS}") {
                    checkout([
                        $class: 'GitSCM',
                        branches: [[name: "${env.YETUS_VERSION}"]],
                        userRemoteConfigs: [[ url: 'https://github.com/apache/yetus']]]
                    )
                }
            }
        }

        stage ('precommit-run') {
            steps {
                withCredentials(
                    [usernamePassword(credentialsId: 'apache-tez-at-github.com',
                                        passwordVariable: 'GITHUB_TOKEN',
                                        usernameVariable: 'GITHUB_USER'),
                                        usernamePassword(credentialsId: 'tez-ci',
                                        passwordVariable: 'JIRA_PASSWORD',
                                        usernameVariable: 'JIRA_USER')]) {
                        sh '''#!/usr/bin/env bash

                        set -e

                        TESTPATCHBIN="${WORKSPACE}/${YETUS}/precommit/src/main/shell/test-patch.sh"

                        # this must be clean for every run
                        if [[ -d "${WORKSPACE}/${PATCHDIR}" ]]; then
                          rm -rf "${WORKSPACE}/${PATCHDIR}"
                        fi
                        mkdir -p "${WORKSPACE}/${PATCHDIR}"

                        YETUS_ARGS+=("--patch-dir=${WORKSPACE}/${PATCHDIR}")

                        # where the source is located
                        YETUS_ARGS+=("--basedir=${WORKSPACE}/${SOURCEDIR}")

                        # our project defaults come from a personality file
                        YETUS_ARGS+=("--project=tez")
                        YETUS_ARGS+=("--personality=${WORKSPACE}/${SOURCEDIR}/.yetus/personality.sh")

                        # lots of different output formats
                        YETUS_ARGS+=("--brief-report-file=${WORKSPACE}/${PATCHDIR}/brief.txt")
                        YETUS_ARGS+=("--console-report-file=${WORKSPACE}/${PATCHDIR}/console.txt")
                        YETUS_ARGS+=("--html-report-file=${WORKSPACE}/${PATCHDIR}/report.html")

                        # enable writing back to Github
                        YETUS_ARGS+=(--github-token="${GITHUB_TOKEN}")

                        # enable writing back to ASF JIRA
                        YETUS_ARGS+=(--jira-password="${JIRA_PASSWORD}")
                        YETUS_ARGS+=(--jira-user="${JIRA_USER}")

                        # enable everything
                        YETUS_ARGS+=("--plugins=all")

                        # auto-kill any surefire stragglers during unit test runs
                        YETUS_ARGS+=("--reapermode=kill")

                        # set relatively high limits for ASF machines
                        # changing these to higher values may cause problems
                        # with other jobs on systemd-enabled machines
                        YETUS_ARGS+=("--proclimit=5500")
                        YETUS_ARGS+=("--dockermemlimit=20g")

                        # -1 findbugs issues that show up prior to the patch being applied
                        # YETUS_ARGS+=("--findbugs-strict-precheck")

                        # rsync these files back into the archive dir
                        YETUS_ARGS+=("--archive-list=checkstyle-errors.xml,findbugsXml.xml")

                        # URL for user-side presentation in reports and such to our artifacts
                        # (needs to match the archive bits below)
                        YETUS_ARGS+=("--build-url-artifacts=artifact/out")

                        # don't let these tests cause -1s because we aren't really paying that
                        # much attention to them
                        YETUS_ARGS+=("--tests-filter=checkstyle")

                        # run in docker mode and specifically point to our
                        # Dockerfile since we don't want to use the auto-pulled version.
                        YETUS_ARGS+=("--docker")
                        YETUS_ARGS+=("--dockerfile=${DOCKERFILE}")
                        YETUS_ARGS+=("--mvn-custom-repos")

                        # help keep the ASF boxes clean
                        YETUS_ARGS+=("--sentinel")

                        # test with Java 8 and 11
                        YETUS_ARGS+=("--java-home=/usr/lib/jvm/java-8-openjdk-amd64")
                        YETUS_ARGS+=("--multijdkdirs=/usr/lib/jvm/java-11-openjdk-amd64")
                        YETUS_ARGS+=("--multijdktests=compile")

                        "${TESTPATCHBIN}" "${YETUS_ARGS[@]}"
                        '''
                }
            }
        }

    }

    post {
        always {
          script {

            // Publish status if it was missed
            withCredentials([usernamePassword(credentialsId: 'apache-tez-at-github.com',
                             passwordVariable: 'GITHUB_TOKEN',
                             usernameVariable: 'GITHUB_USER')]) {
                    sh '''#!/usr/bin/env bash

                    # enable writing back to Github
                    YETUS_ARGS+=(--github-token="${GITHUB_TOKEN}")

                    YETUS_ARGS+=("--patch-dir=${WORKSPACE}/${YETUS_RELATIVE_PATCHDIR}")

                    # run test-patch from the source tree specified up above
                    TESTPATCHBIN="${WORKSPACE}/${YETUS}/precommit/src/main/shell/github-status-recovery.sh"

                    # execute! (we are using bash instead of the
                    # bin in case the perms get messed up)
                    /usr/bin/env bash "${TESTPATCHBIN}" "${YETUS_ARGS[@]}" || true
                    '''
            }

            // Yetus output
            archiveArtifacts "${env.PATCHDIR}/**"
            // Publish the HTML report so that it can be looked at
            // Has to be relative to WORKSPACE.
            publishHTML (target: [
                          allowMissing: true,
                          keepAll: true,
                          alwaysLinkToLastBuild: true,
                          // Has to be relative to WORKSPACE
                          reportDir: "${env.PATCHDIR}",
                          reportFiles: 'report.html',
                          reportName: 'Yetus Report'
            ])
            // Publish JUnit results
            try {
                junit "${env.SOURCEDIR}/**/target/surefire-reports/*.xml"
            } catch(e) {
                echo 'junit processing: ' + e.toString()
            }
          }
        }

        // Jenkins pipeline jobs fill slaves on PRs without this :(
        cleanup() {
            script {
                sh '''
                    # See YETUS-764
                    if [ -f "${WORKSPACE}/${PATCHDIR}/pidfile.txt" ]; then
                      echo "test-patch process appears to still be running: killing"
                      kill `cat "${WORKSPACE}/${PATCHDIR}/pidfile.txt"` || true
                      sleep 10
                    fi
                    if [ -f "${WORKSPACE}/${PATCHDIR}/cidfile.txt" ]; then
                      echo "test-patch container appears to still be running: killing"
                      docker kill `cat "${WORKSPACE}/${PATCHDIR}/cidfile.txt"` || true
                    fi
                    # See HADOOP-13951
                    chmod -R u+rxw "${WORKSPACE}"
                    '''
                deleteDir()
            }
        }
    }
}