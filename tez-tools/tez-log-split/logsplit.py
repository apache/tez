#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
import os
import re
from gzip import GzipFile as GZFile
from getopt import getopt

def usage():
    sys.stderr.write("""
usage: logsplit.py <log-file>

Input files for this tool can be prepared by "yarn logs -applicationId <application_...>".
""")

def open_file(f):
    if f.endswith(".gz"):
        return GZFile(f)
    return open(f)

class AggregatedLog(object):
    def __init__(self):
        self.in_container = False
        self.in_logfile = False
        self.current_container_header = None
        self.current_container_name = None
        self.current_host_name = None # as read from log line: "hello.my.host.com_8041"
        self.current_file = None
        self.HEADER_CONTAINER_RE = re.compile("Container: (container_[a-z0-9_]+) on (.*)")
        self.HEADER_LAST_ROW_RE = re.compile("^LogContents:$")
        self.HEADER_LOG_TYPE_RE = re.compile("^LogType:(.*)")
        self.LAST_LOG_LINE_RE = re.compile("^End of LogType:.*")

    def process(self, input_file):
        self.output_folder = input_file.name + "_splitlogs"
        os.mkdir(self.output_folder)

        for line in input_file:
            self.parse(line)

    def parse(self, line):
        if self.in_container:
            if self.in_logfile:
                m = self.LAST_LOG_LINE_RE.match(line)
                if m:
                    self.in_container = False
                    self.in_logfile = False
                    self.current_file.close()
                else:
                    self.write_to_current_file(line)
            else:
                m = self.HEADER_LOG_TYPE_RE.match(line)
                if m:
                    file_name = m.group(1)
                    self.create_file_in_current_container(file_name)
                elif self.HEADER_LAST_ROW_RE.match(line):
                    self.in_logfile = True
                    self.write_to_current_file(self.current_container_header) #for host reference
        else:
            m = self.HEADER_CONTAINER_RE.match(line)
            self.current_container_header = line
            if m:
                self.in_container = True
                self.current_container_name = m.group(1)
                self.current_host_name = m.group(2)
                self.start_container_folder()

    def start_container_folder(self):
        container_dir = os.path.join(self.output_folder, self.get_current_container_dir_name())
        if not os.path.exists(container_dir):
            os.makedirs(container_dir)

    def create_file_in_current_container(self, file_name):
        file_to_be_created = os.path.join(self.output_folder, self.get_current_container_dir_name(), file_name)
        file = open(file_to_be_created, "w+")
        self.current_file = file

    def write_to_current_file(self, line):
        self.current_file.write(line)

    def get_current_container_dir_name(self):
        return os.path.join(self.current_host_name, self.current_container_name)

def main(argv):
    (opts, args) = getopt(argv, "")
    input_file = args[0]
    fp = open_file(input_file)
    aggregated_log = AggregatedLog()
    aggregated_log.process(fp)
    print ("Split application logs was written into folder " + aggregated_log.output_folder)
    fp.close()

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
