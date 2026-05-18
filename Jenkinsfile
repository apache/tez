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

                    cd "${WORKSPACE}/${SOURCEDIR}"
                    # Ensure origin/master is available for diffing
                    git fetch origin master || true
                    git diff origin/master...HEAD > "${WORKSPACE}/${PATCHDIR}/local-pr.patch"
                    cd "${WORKSPACE}"

                    YETUS_ARGS=()

                    # Uncomment the following line to enable Yetus debugging
                    # YETUS_ARGS+=("--debug")

                    # 1. Target Issue/PR
                    if [[ -n "${JIRA_ISSUE_KEY}" ]]; then
                        YETUS_ARGS+=("${JIRA_ISSUE_KEY}")
                    elif [[ -n "${CHANGE_ID}" ]]; then
                        echo "Processing PR: ${CHANGE_ID} using local patch"
                    else
                        echo "No PR or JIRA key provided. Skipping Yetus."
                        exit 0
                    fi

                    # 2. Core Paths & Project
                    YETUS_ARGS+=("--patch-dir=${WORKSPACE}/${PATCHDIR}")
                    YETUS_ARGS+=("--basedir=${WORKSPACE}/${SOURCEDIR}")
                    YETUS_ARGS+=("--project=tez")

                    # 3. Docker & Environment
                    YETUS_ARGS+=("--docker")
                    YETUS_ARGS+=("--dockerfile=${DOCKERFILE}")
                    YETUS_ARGS+=("--java-home=/opt/java/openjdk")
                    YETUS_ARGS+=("--build-url-artifacts=artifact/out")

                    # 4. Plugins & Personality
                    # Copy personality to PATCHDIR to avoid deletion by Yetus' robot mode
                    cp "${WORKSPACE}/${SOURCEDIR}/dev-support/tez-personality.sh" "${WORKSPACE}/${PATCHDIR}/tez-personality.sh"

                    # Disable mvninstall Yetus plugin as Github Actions is already running
                    # The maven install phase
                    YETUS_ARGS+=("--plugins=all,-mvninstall")

                    YETUS_ARGS+=("--personality=${WORKSPACE}/${PATCHDIR}/tez-personality.sh")
                    YETUS_ARGS+=("--mvn-custom-repos")
                    YETUS_ARGS+=("--tests-filter=checkstyle")
                    YETUS_ARGS+=("--archive-list=checkstyle-errors.xml,spotbugsXml.xml")
                    YETUS_ARGS+=("--reapermode=kill")
                    YETUS_ARGS+=("--sentinel")

                    # 5. Reporting & GitHub Integration
                    YETUS_ARGS+=("--github-token=${GITHUB_TOKEN}")
                    YETUS_ARGS+=("--github-write-comment")
                    YETUS_ARGS+=("--github-use-emoji-vote")
                    YETUS_ARGS+=("--html-report-file=${WORKSPACE}/${PATCHDIR}/report.html")
                    YETUS_ARGS+=("--console-report-file=${WORKSPACE}/${PATCHDIR}/console.txt")
                    YETUS_ARGS+=("--brief-report-file=${WORKSPACE}/${PATCHDIR}/brief.txt")

                    echo "Running Yetus with local patch overriding GitHub..."
                    "${TESTPATCHBIN}" "${YETUS_ARGS[@]}" "${WORKSPACE}/${PATCHDIR}/local-pr.patch"
                    '''
                }
            }
        }
    }

    post {
        always {
            script {
                sh '''
                    # Fix permissions before attempting to read/zip directories created by root Docker processes
                    echo "Cleaning up workspace permissions to prevent HADOOP-13951 and unzipping errors..."
                    chmod -R u+rxw "${WORKSPACE}" || true

                    # Zip test logs for easier debugging and storage efficiency
                    cd "${SOURCEDIR}" || exit 0
                    find . -type d -name "surefire-reports" | while read -r dir; do
                        rel_path=$(echo "${dir#./}" | tr '/' '_')
                        zip -r -q "../${PATCHDIR}/test-logs-${rel_path}.zip" "${dir}" || true
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
                // Wipe the workspace to prevent Jenkins agent disk exhaustion
                deleteDir()
            }
        }
    }
}
