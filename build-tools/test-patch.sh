#!/usr/bin/env bash
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


#set -x

### Setup some variables.
### BUILD_URL is set by Hudson if it is run by patch process
### Read variables from properties file
bindir=$(dirname $0)

# Defaults
if [ -z "$MAVEN_HOME" ]; then
  MVN=mvn
else
  MVN=$MAVEN_HOME/bin/mvn
fi

PROJECT_NAME=Tez
JENKINS=false
PATCH_DIR=/tmp
BASEDIR=$(pwd)
PS=${PS:-ps}
AWK=${AWK:-awk}
WGET=${WGET:-wget}
GIT=${GIT:-git}
GREP=${GREP:-grep}
PATCH=${PATCH:-patch}
DIFF=${DIFF:-diff}
JIRACLI=${JIRA:-jira}
SED=${SED:-sed}
CURL=${CURL:-curl}
SPOTBUGS_HOME=${SPOTBUGS_HOME}

###############################################################################
printUsage() {
  echo "Usage: $0 [options] patch-file | defect-number"
  echo
  echo "Where:"
  echo "  patch-file is a local patch file containing the changes to test"
  echo "  defect-number is a JIRA defect number (e.g. 'TEZ-1234') to test (Jenkins only)"
  echo
  echo "Options:"
  echo "--patch-dir=<dir>      The directory for working and output files (default '/tmp')"
  echo "--basedir=<dir>        The directory to apply the patch to (default current directory)"
  echo "--mvn-cmd=<cmd>        The 'mvn' command to use (default \$MAVEN_HOME/bin/mvn, or 'mvn')"
  echo "--ps-cmd=<cmd>         The 'ps' command to use (default 'ps')"
  echo "--awk-cmd=<cmd>        The 'awk' command to use (default 'awk')"
  echo "--git-cmd=<cmd>        The 'git' command to use (default 'git')"
  echo "--grep-cmd=<cmd>       The 'grep' command to use (default 'grep')"
  echo "--patch-cmd=<cmd>      The 'patch' command to use (default 'patch')"
  echo "--diff-cmd=<cmd>       The 'diff' command to use (default 'diff')"
  echo "--spotbugs-home=<path> Spotbugs home directory (default SPOTBUGS_HOME environment variable)"
  echo "--dirty-workspace      Allow the local git workspace to have uncommitted changes"
  echo "--run-tests            Run all tests below the base directory"
  echo
  echo "Jenkins-only options:"
  echo "--jenkins              Run by Jenkins (runs tests and posts results to JIRA)"
  echo "--wget-cmd=<cmd>       The 'wget' command to use (default 'wget')"
  echo "--jira-cmd=<cmd>       The 'jira' command to use (default 'jira')"
  echo "--jira-password=<pw>   The password for the 'jira' command"
}

###############################################################################
parseArgs() {
  for i in $*
  do
    case $i in
    --jenkins)
      JENKINS=true
      ;;
    --patch-dir=*)
      PATCH_DIR=${i#*=}
      ;;
    --basedir=*)
      BASEDIR=${i#*=}
      ;;
    --mvn-cmd=*)
      MVN=${i#*=}
      ;;
    --ps-cmd=*)
      PS=${i#*=}
      ;;
    --awk-cmd=*)
      AWK=${i#*=}
      ;;
    --wget-cmd=*)
      WGET=${i#*=}
      ;;
    --git-cmd=*)
      GIT=${i#*=}
      ;;
    --grep-cmd=*)
      GREP=${i#*=}
      ;;
    --patch-cmd=*)
      PATCH=${i#*=}
      ;;
    --diff-cmd=*)
      DIFF=${i#*=}
      ;;
    --jira-cmd=*)
      JIRACLI=${i#*=}
      ;;
    --jira-password=*)
      JIRA_PASSWD=${i#*=}
      ;;
    --spotbugs-home=*)
      SPOTBUGS_HOME=${i#*=}
      ;;
    --dirty-workspace)
      DIRTY_WORKSPACE=true
      ;;
    --run-tests)
      RUN_TESTS=true
      ;;
    *)
      PATCH_OR_DEFECT=$i
      ;;
    esac
  done
  if [ -z "$PATCH_OR_DEFECT" ]; then
    printUsage
    exit 1
  fi
  if [[ $JENKINS == "true" ]] ; then
    echo "Running in Jenkins mode"
    defect=$PATCH_OR_DEFECT
  else
    echo "Running in developer mode"
    JENKINS=false
    ### PATCH_FILE contains the location of the patchfile
    PATCH_FILE=$PATCH_OR_DEFECT
    if [[ ! -e "$PATCH_FILE" ]] ; then
      echo "Unable to locate the patch file $PATCH_FILE"
      cleanupAndExit 0
    fi
    ### Check if $PATCH_DIR exists. If it does not exist, create a new directory
    if [[ ! -e "$PATCH_DIR" ]] ; then
      mkdir "$PATCH_DIR"
      if [[ $? == 0 ]] ; then
        echo "$PATCH_DIR has been created"
      else
        echo "Unable to create $PATCH_DIR"
        cleanupAndExit 0
      fi
    fi
    ### Obtain the patch filename to append it to the version number
    defect=`basename $PATCH_FILE`
  fi
}

###############################################################################
checkout () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Testing patch for ${defect}."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  ### When run by a developer, if the workspace contains modifications, do not continue
  ### unless the --dirty-workspace option was set
  status=`$GIT status --porcelain`
  if [[ $JENKINS == "false" ]] ; then
    if [[ "$status" != "" && -z $DIRTY_WORKSPACE ]] ; then
      echo "ERROR: can't run in a workspace that contains the following modifications"
      echo "$status"
      cleanupAndExit 1
    fi
    echo
  else
    cd $BASEDIR
    $GIT reset --hard
    $GIT clean -xdf
    $GIT checkout master
    $GIT pull --rebase
  fi
  GIT_REVISION=`git rev-parse --verify --short HEAD`
  return $?
}

###############################################################################
downloadPatch () {
  ### Download latest patch file (ignoring .htm and .html) when run from patch process
  if [[ $JENKINS == "true" ]] ; then
    $WGET -q -O $PATCH_DIR/jira http://issues.apache.org/jira/browse/$defect
    if [[ `$GREP -c 'Patch Available' $PATCH_DIR/jira` == 0 ]] ; then
      echo "$defect is not \"Patch Available\".  Exiting."
      cleanupAndExit 0
    fi
    relativePatchURL=`$GREP -o '"/jira/secure/attachment/[0-9]*/[^"]*' $PATCH_DIR/jira | $GREP -v -e 'htm[l]*$' | sort | tail -1 | $GREP -o '/jira/secure/attachment/[0-9]*/[^"]*'`
    patchURL="http://issues.apache.org${relativePatchURL}"
    patchNum=`echo $patchURL | $GREP -o '[0-9]*/' | $GREP -o '[0-9]*'`
    echo "$defect patch is being downloaded at `date` from"
    echo "$patchURL"
    $WGET -q -O $PATCH_DIR/patch $patchURL
    VERSION=${GIT_REVISION}_${defect}_PATCH-${patchNum}
    JIRA_COMMENT="Here are the results of testing the latest attachment
  $patchURL
  against master revision ${GIT_REVISION}."

  ### Copy the patch file to $PATCH_DIR
  else
    VERSION=PATCH-${defect}
    cp $PATCH_FILE $PATCH_DIR/patch
    if [[ $? == 0 ]] ; then
      echo "Patch file $PATCH_FILE copied to $PATCH_DIR"
    else
      echo "Could not copy $PATCH_FILE to $PATCH_DIR"
      cleanupAndExit 0
    fi
  fi
}

###############################################################################
verifyPatch () {
  # Before building, check to make sure that the patch is valid
  $bindir/smart-apply-patch.sh $PATCH_DIR/patch dryrun
  if [[ $? != 0 ]] ; then
    echo "PATCH APPLICATION FAILED"
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 patch{color}.  The patch command could not apply the patch."
    return 1
  else
    return 0
  fi
}

###############################################################################
prebuildWithoutPatch () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo " Pre-build master to verify master stability and javac warnings"
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "Compiling $(pwd)"
  echo "$MVN clean test -DskipTests -Ptest-patch > $PATCH_DIR/masterJavacWarnings.txt 2>&1"
  $MVN clean test -DskipTests -Ptest-patch > $PATCH_DIR/masterJavacWarnings.txt 2>&1
  if [[ $? != 0 ]] ; then
    echo "master compilation is broken?"
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 patch{color}.  master compilation may be broken."
    return 1
  fi

  echo "$MVN clean test javadoc:javadoc -DskipTests > $PATCH_DIR/masterJavadocWarnings.txt 2>&1"
  $MVN clean test javadoc:javadoc -DskipTests > $PATCH_DIR/masterJavadocWarnings.txt 2>&1
  if [[ $? != 0 ]] ; then
    echo "master javadoc compilation is broken?"
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 patch{color}.  master compilation may be broken."
    return 1
  fi

  return 0
}

###############################################################################
### Check for @author tags in the patch
checkAuthor () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking there are no @author tags in the patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  authorTags=`$GREP -c -i '@author' $PATCH_DIR/patch`
  echo "There appear to be $authorTags @author tags in the patch."
  if [[ $authorTags != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 @author{color}.  The patch appears to contain $authorTags @author tags which the Tez community has agreed to not allow in code contributions."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 @author{color}.  The patch does not contain any @author tags."
  return 0
}

###############################################################################
### Check for tests in the patch
checkTests () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Checking there are new or changed tests in the patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  testReferences=`$GREP -c -i -e '^+++.*/test' $PATCH_DIR/patch`
  echo "There appear to be $testReferences test files referenced in the patch."
  if [[ $testReferences == 0 ]] ; then
    if [[ $JENKINS == "true" ]] ; then
      patchIsDoc=`$GREP -c -i 'title="documentation' $PATCH_DIR/jira`
      if [[ $patchIsDoc != 0 ]] ; then
        echo "The patch appears to be a documentation patch that doesn't require tests."
        JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+0 tests included{color}.  The patch appears to be a documentation patch that doesn't require tests."
        return 0
      fi
    fi
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 tests included{color}.  The patch doesn't appear to include any new or modified tests.
                        Please justify why no new tests are needed for this patch.
                        Also please list what manual steps were performed to verify this patch."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 tests included{color}.  The patch appears to include $testReferences new or modified test files."
  return 0
}

cleanUpXml () {
  cd $BASEDIR/conf
  for file in `ls *.xml.template`
    do
      rm -f `basename $file .template`
    done
  cd $BASEDIR
}

###############################################################################
### Attempt to apply the patch
applyPatch () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Applying patch."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  export PATCH
  $bindir/smart-apply-patch.sh $PATCH_DIR/patch
  if [[ $? != 0 ]] ; then
    echo "PATCH APPLICATION FAILED"
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 patch{color}.  The patch command could not apply the patch."
    return 1
  fi
  return 0
}

###############################################################################
calculateJavadocWarnings() {
    WARNING_FILE="$1"
    RET=$(egrep "^[0-9]+ warnings?$" "$WARNING_FILE" | awk '{sum+=$1} END {print sum}')
}

### Check there are no javadoc warnings
checkJavadocWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched javadoc warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN clean test javadoc:javadoc -DskipTests > $PATCH_DIR/patchJavadocWarnings.txt 2>&1"
  $MVN clean test javadoc:javadoc -DskipTests > $PATCH_DIR/patchJavadocWarnings.txt 2>&1
  calculateJavadocWarnings "$PATCH_DIR/masterJavadocWarnings.txt"
  nummasterJavadocWarnings=$RET
  calculateJavadocWarnings "$PATCH_DIR/patchJavadocWarnings.txt"
  numPatchJavadocWarnings=$RET
  grep -i warning "$PATCH_DIR/masterJavadocWarnings.txt" > "$PATCH_DIR/masterJavadocWarningsFiltered.txt"
  grep -i warning "$PATCH_DIR/patchJavadocWarnings.txt" > "$PATCH_DIR/patchJavadocWarningsFiltered.txt"
  diff -u "$PATCH_DIR/masterJavadocWarningsFiltered.txt" \
          "$PATCH_DIR/patchJavadocWarningsFiltered.txt" > \
          "$PATCH_DIR/diffJavadocWarnings.txt"
  rm -f "$PATCH_DIR/masterJavadocWarningsFiltered.txt" "$PATCH_DIR/patchJavadocWarningsFiltered.txt"
  echo "There appear to be $nummasterJavadocWarnings javadoc warnings before the patch and $numPatchJavadocWarnings javadoc warnings after applying the patch."
  if [[ $nummasterJavadocWarnings != "" && $numPatchJavadocWarnings != "" ]] ; then
    if [[ $numPatchJavadocWarnings -gt $nummasterJavadocWarnings ]] ; then
      JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 javadoc{color}.  The javadoc tool appears to have generated `expr $(($numPatchJavadocWarnings-$nummasterJavadocWarnings))` warning messages.
        See $BUILD_URL/artifact/patchprocess/diffJavadocWarnings.txt for details."
        return 1
    fi
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 javadoc{color}.  There were no new javadoc warning messages."
  return 0
}

###############################################################################
### Check there are no changes in the number of Javac warnings
checkJavacWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched javac warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN clean test -DskipTests -Ptest-patch > $PATCH_DIR/patchJavacWarnings.txt 2>&1"
  $MVN clean test -DskipTests -Ptest-patch > $PATCH_DIR/patchJavacWarnings.txt 2>&1
  if [[ $? != 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 javac{color:red}.  The patch appears to cause the build to fail."
    return 2
  fi
  ### Compare master and patch javac warning numbers
  if [[ -f $PATCH_DIR/patchJavacWarnings.txt ]] ; then
    $GREP '\[WARNING\]' $PATCH_DIR/masterJavacWarnings.txt > $PATCH_DIR/filteredmasterJavacWarnings.txt
    $GREP '\[WARNING\]' $PATCH_DIR/patchJavacWarnings.txt > $PATCH_DIR/filteredPatchJavacWarnings.txt
    masterJavacWarnings=`cat $PATCH_DIR/filteredmasterJavacWarnings.txt | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
    patchJavacWarnings=`cat $PATCH_DIR/filteredPatchJavacWarnings.txt | $AWK 'BEGIN {total = 0} {total += 1} END {print total}'`
    echo "There appear to be $masterJavacWarnings javac compiler warnings before the patch and $patchJavacWarnings javac compiler warnings after applying the patch."
    if [[ $patchJavacWarnings != "" && $masterJavacWarnings != "" ]] ; then
      if [[ $patchJavacWarnings -gt $masterJavacWarnings ]] ; then
        JIRA_COMMENT="$JIRA_COMMENT

      {color:red}-1 javac{color}.  The applied patch generated $patchJavacWarnings javac compiler warnings (more than the master's current $masterJavacWarnings warnings)."

    $DIFF $PATCH_DIR/filteredmasterJavacWarnings.txt $PATCH_DIR/filteredPatchJavacWarnings.txt > $PATCH_DIR/diffJavacWarnings.txt
        JIRA_COMMENT_FOOTER="Javac warnings: $BUILD_URL/artifact/patchprocess/diffJavacWarnings.txt
$JIRA_COMMENT_FOOTER"

        return 1
      fi
    fi
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 javac{color}.  The applied patch does not increase the total number of javac compiler warnings."
  return 0
}

###############################################################################
### Check there are no changes in the number of release audit (RAT) warnings
checkReleaseAuditWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched release audit warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN apache-rat:check > $PATCH_DIR/patchReleaseAuditOutput.txt 2>&1"
  $MVN apache-rat:check > $PATCH_DIR/patchReleaseAuditOutput.txt 2>&1
  find $BASEDIR -name rat.txt | xargs cat > $PATCH_DIR/patchReleaseAuditWarnings.txt

  ### Compare master and patch release audit warning numbers
  if [[ -f $PATCH_DIR/patchReleaseAuditWarnings.txt ]] ; then
    patchReleaseAuditWarnings=`$GREP -c '\!?????' $PATCH_DIR/patchReleaseAuditWarnings.txt`
    echo ""
    echo ""
    echo "There appear to be $patchReleaseAuditWarnings release audit warnings after applying the patch."
    if [[ $patchReleaseAuditWarnings != "" ]] ; then
      if [[ $patchReleaseAuditWarnings -gt 0 ]] ; then
        JIRA_COMMENT="$JIRA_COMMENT

        {color:red}-1 release audit{color}.  The applied patch generated $patchReleaseAuditWarnings release audit warnings."
        $GREP '\!?????' $PATCH_DIR/patchReleaseAuditWarnings.txt > $PATCH_DIR/patchReleaseAuditProblems.txt
        echo "Lines that start with ????? in the release audit report indicate files that do not have an Apache license header." >> $PATCH_DIR/patchReleaseAuditProblems.txt
        JIRA_COMMENT_FOOTER="Release audit warnings: $BUILD_URL/artifact/patchprocess/patchReleaseAuditProblems.txt
$JIRA_COMMENT_FOOTER"
        return 1
      fi
    fi
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 release audit{color}.  The applied patch does not increase the total number of release audit warnings."
  return 0
}


###############################################################################
### Install the new jars so tests and spotbugs can find all of the updated jars
buildAndInstall () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Installing all of the jars"
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  echo "$MVN install -Dmaven.javadoc.skip=true -DskipTests -D${PROJECT_NAME}PatchProcess"
  $MVN install -Dmaven.javadoc.skip=true -DskipTests -D${PROJECT_NAME}PatchProcess
  return $?
}


###############################################################################
### Check there are no changes in the number of Spotbugs warnings
checkSpotbugsWarnings () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Determining number of patched Spotbugs warnings."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""

  rc=0
  echo " Running spotbugs "
  echo "$MVN clean test spotbugs:spotbugs -DskipTests < /dev/null > $PATCH_DIR/patchSpotBugsOutput.txt 2>&1"
  $MVN clean test spotbugs:spotbugs -DskipTests < /dev/null > $PATCH_DIR/patchSpotBugsOutput.txt 2>&1
  rc=$?
  spotbugs_version=$(${AWK} 'match($0, /spotbugs-maven-plugin:[^:]*:spotbugs/) { print substr($0, RSTART + 22, RLENGTH - 31); exit }' "${PATCH_DIR}/patchSpotBugsOutput.txt")

  if [ $rc != 0 ] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 spotbugs{color}.  The patch appears to cause Spotbugs (version ${spotbugs_version}) to fail."
    return 1
  fi

  spotbugsWarnings=0
  for file in $(find $BASEDIR -name spotbugsXml.xml)
  do
    relative_file=${file#$BASEDIR/} # strip leading $BASEDIR prefix
    if [ ! $relative_file == "target/spotbugsXml.xml" ]; then
      module_suffix=${relative_file%/target/spotbugsXml.xml} # strip trailing path
      module_suffix=`basename ${module_suffix}`
    fi

    cp $file $PATCH_DIR/patchSpotbugsWarnings${module_suffix}.xml
    $SPOTBUGS_HOME/bin/setBugDatabaseInfo -timestamp "01/01/2000" \
      $PATCH_DIR/patchSpotbugsWarnings${module_suffix}.xml \
      $PATCH_DIR/patchSpotbugsWarnings${module_suffix}.xml
    newSpotbugsWarnings=`$SPOTBUGS_HOME/bin/filterBugs -first "01/01/2000" $PATCH_DIR/patchSpotbugsWarnings${module_suffix}.xml \
      $PATCH_DIR/newPatchSpotbugsWarnings${module_suffix}.xml | $AWK '{print $1}'`
    echo "Found $newSpotbugsWarnings Spotbugs warnings ($file)"
    spotbugsWarnings=$((spotbugsWarnings+newSpotbugsWarnings))
    $SPOTBUGS_HOME/bin/convertXmlToText -html \
      $PATCH_DIR/newPatchSpotbugsWarnings${module_suffix}.xml \
      $PATCH_DIR/newPatchSpotbugsWarnings${module_suffix}.html
    if [[ $newSpotbugsWarnings > 0 ]] ; then
      JIRA_COMMENT_FOOTER="Spotbugs warnings: $BUILD_URL/artifact/patchprocess/newPatchSpotbugsWarnings${module_suffix}.html
$JIRA_COMMENT_FOOTER"
    fi
  done

  if [[ $spotbugsWarnings -gt 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:red}-1 spotbugs{color}.  The patch appears to introduce $spotbugsWarnings new Spotbugs (version ${spotbugs_version}) warnings."
    return 1
  fi
  JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 spotbugs{color}.  The patch does not introduce any new Spotbugs (version ${spotbugs_version}) warnings."
  return 0
}

###############################################################################
### Run the tests
runTests () {
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Running tests."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""

  failed_tests=""
  failed_test_builds=""
  test_timeouts=""
  test_logfile=$PATCH_DIR/testrun.txt
  echo "  Running tests "
  echo "  $MVN clean install -fn -D${PROJECT_NAME}PatchProcess"
  $MVN clean install -fn > $test_logfile 2>&1
  test_build_result=$?
  cat $test_logfile
  module_test_timeouts=`$AWK '/^Running / { if (last) { print last } last=$2 } /^Tests run: / { last="" }' $test_logfile`
  if [[ -n "$module_test_timeouts" ]] ; then
    test_timeouts="$test_timeouts $module_test_timeouts"
  fi
  module_failed_tests=`find . -name 'TEST*.xml' | xargs $GREP  -l -E "<failure|<error" | sed -e "s|.*target/surefire-reports/TEST-|                  |g" | sed -e "s|\.xml||g"`
  if [[ -n "$module_failed_tests" ]] ; then
    failed_tests="${failed_tests} ${module_failed_tests}"
  fi
  if [[ $test_build_result != 0 && -z "$module_failed_tests" && -z "$module_test_timeouts" ]] ; then
    failed_test_builds="$module $failed_test_builds"
  fi
  result=0
  comment_prefix="    {color:red}-1 core tests{color}."
  if [[ -n "$failed_tests" ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

$comment_prefix  The patch failed these unit tests in $modules:
$failed_tests"
    comment_prefix="                                    "
    result=1
  fi
  if [[ -n "$test_timeouts" ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

$comment_prefix  The following test timeouts occurred in $modules:
$test_timeouts"
    comment_prefix="                                    "
    result=1
  fi
  if [[ -n "$failed_test_builds" ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

$comment_prefix  The test build failed in $failed_test_builds"
    result=1
  fi
  if [[ $result == 0 ]] ; then
    JIRA_COMMENT="$JIRA_COMMENT

    {color:green}+1 core tests{color}.  The patch passed unit tests in $modules."
  fi
  return $result
}

###############################################################################
# Find the maven module containing the given file.
findModule (){
 dir=`dirname $1`
 while [ 1 ]
 do
  if [ -f "$dir/pom.xml" ]
  then
    echo $dir
    return
  else
    dir=`dirname $dir`
  fi
 done
}

###############################################################################
### Submit a comment to the defect's Jira
submitJiraComment () {
  local result=$1
  ### Do not output the value of JIRA_COMMENT_FOOTER when run by a developer
  if [[  $JENKINS == "false" ]] ; then
    JIRA_COMMENT_FOOTER=""
  fi
  if [[ $result == 0 ]] ; then
    comment="{color:green}+1 overall{color}.  $JIRA_COMMENT

$JIRA_COMMENT_FOOTER"
  else
    comment="{color:red}-1 overall{color}.  $JIRA_COMMENT

$JIRA_COMMENT_FOOTER"
  fi
  ### Output the test result to the console
  echo "



$comment"

  if [[ $JENKINS == "true" ]] ; then
    echo ""
    echo ""
    echo "======================================================================"
    echo "======================================================================"
    echo "    Adding comment to Jira."
    echo "======================================================================"
    echo "======================================================================"
    echo ""
    echo ""

    # RESTify the comment
    jsoncomment=$(echo "$comment" \
      | ${SED} -e 's,\\,\\\\,g' \
        -e 's,\",\\\",g' \
        -e 's,$,\\r\\n,g' \
      | tr -d '\n')
    jsoncomment='{"body":"'"$jsoncomment"'"}'

    ### Update Jira with a comment
    ${CURL} -X POST \
      -H "Accept: application/json" \
      -H "Content-Type: application/json" \
      -u "tezqa:${JIRA_PASSWD}" \
      -d "$jsoncomment" \
      --silent --location \
      "https://issues.apache.org/jira/rest/api/2/issue/${defect}/comment" \
      >/dev/null
  fi
}

###############################################################################
### Cleanup files
cleanupAndExit () {
  local result=$1
  if [[ $JENKINS == "true" ]] ; then
    if [ -e "$PATCH_DIR" ] ; then
      mv $PATCH_DIR $BASEDIR
    fi
  fi
  echo ""
  echo ""
  echo "======================================================================"
  echo "======================================================================"
  echo "    Finished build."
  echo "======================================================================"
  echo "======================================================================"
  echo ""
  echo ""
  exit $result
}

###############################################################################
###############################################################################
###############################################################################

JIRA_COMMENT=""
JIRA_COMMENT_FOOTER="Console output: $BUILD_URL/console

This message is automatically generated."

### Check if arguments to the script have been specified properly or not
parseArgs $@
cd $BASEDIR

checkout
RESULT=$?
if [[ $JENKINS == "true" ]] ; then
  if [[ $RESULT != 0 ]] ; then
    exit 100
  fi
fi
downloadPatch
verifyPatch
(( RESULT = RESULT + $? ))
if [[ $RESULT != 0 ]] ; then
  submitJiraComment 1
  cleanupAndExit 1
fi
prebuildWithoutPatch
(( RESULT = RESULT + $? ))
if [[ $RESULT != 0 ]] ; then
  submitJiraComment 1
  cleanupAndExit 1
fi
checkAuthor
(( RESULT = RESULT + $? ))

if [[ $JENKINS == "true" ]] ; then
  cleanUpXml
fi
checkTests
(( RESULT = RESULT + $? ))
applyPatch
APPLY_PATCH_RET=$?
(( RESULT = RESULT + $APPLY_PATCH_RET ))
if [[ $APPLY_PATCH_RET != 0 ]] ; then
  submitJiraComment 1
  cleanupAndExit 1
fi
checkJavacWarnings
JAVAC_RET=$?
#2 is returned if the code could not compile
if [[ $JAVAC_RET == 2 ]] ; then
  submitJiraComment 1
  cleanupAndExit 1
fi
(( RESULT = RESULT + $JAVAC_RET ))
checkJavadocWarnings
(( RESULT = RESULT + $? ))
buildAndInstall
checkSpotbugsWarnings
(( RESULT = RESULT + $? ))
checkReleaseAuditWarnings
(( RESULT = RESULT + $? ))
### Run tests for Jenkins or if explictly asked for by a developer
if [[ $JENKINS == "true" || $RUN_TESTS == "true" ]] ; then
  runTests
  (( RESULT = RESULT + $? ))
fi
JIRA_COMMENT_FOOTER="Test results: $BUILD_URL/testReport/
$JIRA_COMMENT_FOOTER"

submitJiraComment $RESULT
cleanupAndExit $RESULT
