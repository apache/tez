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
        timeout(time: 8, unit: 'HOURS')
        timestamps()
        checkoutToSubdirectory('src')
        // Ensure only one build runs per PR at a time
        disableConcurrentBuilds(abortPrevious: true)
    }

    parameters {
        string(name: 'JIRA_ISSUE_KEY',
               defaultValue: '',
               description: 'The JIRA issue to comment on. Example: TEZ-1234')
    }

    environment {
        SOURCEDIR = 'src'
        YETUSDIR = 'yetus'
        PATCHDIR = 'out'
        DOCKERFILE = "${SOURCEDIR}/build-tools/docker/Dockerfile"

        // Credentials IDs
        GITHUB_CRED_ID = 'apache-tez-at-github.com'
        JIRA_CRED_ID = 'tez-ci'

        // Yetus version
        YETUS_VERSION = 'rel/0.15.1'
    }

    stages {
        stage('Install Yetus') {
            steps {
                dir("${WORKSPACE}/${env.YETUSDIR}") {
                    checkout([
                        $class: 'GitSCM',
                        branches: [[name: "${env.YETUS_VERSION}"]],
                        userRemoteConfigs: [[url: 'https://github.com/apache/yetus']]]
                    )
                }
            }
        }

        stage('Yetus Analysis') {
            steps {
                withCredentials([
                    usernamePassword(credentialsId: env.GITHUB_CRED_ID, passwordVariable: 'GITHUB_TOKEN', usernameVariable: 'GITHUB_USER'),
                    usernamePassword(credentialsId: env.JIRA_CRED_ID, passwordVariable: 'JIRA_PASSWORD', usernameVariable: 'JIRA_USER')
                ]) {
                    sh '''#!/usr/bin/env bash
                    set -e

                    TESTPATCHBIN="${WORKSPACE}/${YETUSDIR}/precommit/src/main/shell/test-patch.sh"

                    rm -rf "${WORKSPACE}/${PATCHDIR}"
                    mkdir -p "${WORKSPACE}/${PATCHDIR}"

                    YETUS_ARGS=()

                    if [[ -n "${JIRA_ISSUE_KEY}" ]]; then
                        YETUS_ARGS+=("${JIRA_ISSUE_KEY}")
                    elif [[ -n "${CHANGE_URL}" ]]; then
                        YETUS_ARGS+=("${CHANGE_URL}")
                    else
                        echo "No PR or JIRA key provided. Skipping Yetus."
                        exit 0
                    fi

                    YETUS_ARGS+=("--patch-dir=${WORKSPACE}/${PATCHDIR}")
                    YETUS_ARGS+=("--basedir=${WORKSPACE}/${SOURCEDIR}")
                    YETUS_ARGS+=("--project=tez")
                    YETUS_ARGS+=("--personality=${WORKSPACE}/${SOURCEDIR}/dev-support/tez-personality.sh")
                    YETUS_ARGS+=("--dockerfile=${DOCKERFILE}")

                    # Reporting
                    YETUS_ARGS+=("--html-report-file=${WORKSPACE}/${PATCHDIR}/report.html")
                    YETUS_ARGS+=("--console-report-file=${WORKSPACE}/${PATCHDIR}/console.txt")
                    YETUS_ARGS+=("--brief-report-file=${WORKSPACE}/${PATCHDIR}/brief.txt")

                    echo "Running Yetus with: ${YETUS_ARGS[*]}"
                    "${TESTPATCHBIN}" "${YETUS_ARGS[@]}"
                    '''
                }
            }
        }
    }

    post {
        always {
            script {
                // Logic to zip test logs for easier debugging and storage efficiency
                // Move them to PATCHDIR so they are archived by archiveArtifacts
                sh '''
                    find "${SOURCEDIR}" -name "surefire-reports" -type d | while read -r dir; do
                        rel_path=$(echo "${dir}" | sed "s|^${SOURCEDIR}/||" | tr '/' '_')
                        zip -r -q "${PATCHDIR}/test-logs-${rel_path}.zip" "${dir}" || true
                    done
                '''

                dir(env.SOURCEDIR) {
                    // Safely publish junit results
                    junit testResults: "**/target/surefire-reports/*.xml", allowEmptyResults: true
                }

                // Archive Yetus output, reports, and zipped test logs
                archiveArtifacts artifacts: "${env.PATCHDIR}/**", allowEmptyArchive: true

                // Publish Yetus HTML report to Jenkins UI sidebar only if it exists
                if (fileExists("${env.PATCHDIR}/report.html")) {
                    publishHTML(target: [
                        allowMissing: true,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: "${env.PATCHDIR}",
                        reportFiles: 'report.html',
                        reportName: 'Yetus Report'
                    ])
                }
            }
        }

        cleanup {
            script {
                sh '''
                    echo "Cleaning up workspace permissions to prevent HADOOP-13951..."
                    chmod -R u+rxw "${WORKSPACE}" || true
                '''
                // Wipe the workspace to prevent Jenkins agent disk exhaustion
                deleteDir()
            }
        }
    }
}
