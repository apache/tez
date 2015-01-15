/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.mapreduce.examples.helpers;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Preconditions;

public class SplitsInClientOptionParser {

  private CommandLine cmdLine;
  private String[] otherArgs;

  private boolean parsed = false;

  public SplitsInClientOptionParser() {

  }

  public String[] getRemainingArgs() {
    Preconditions.checkState(parsed,
        "Cannot get remaining args without parsing");
    return otherArgs.clone();
  }

  @SuppressWarnings("static-access")
  public boolean parse(String[] args, boolean defaultVal) throws ParseException {
    Preconditions.checkState(parsed == false,
        "Craete a new instance for different option sets");
    parsed = true;
    Options opts = new Options();
    Option opt = OptionBuilder
        .withArgName("splits_in_client")
        .hasArg()
        .withDescription(
            "specify whether splits should be generated in the client")
        .create("generateSplitsInClient");
    opts.addOption(opt);
    CommandLineParser parser = new GnuParser();

    cmdLine = parser.parse(opts, args, false);
    if (cmdLine.hasOption("generateSplitsInClient")) {
      defaultVal = Boolean.parseBoolean(cmdLine
          .getOptionValue("generateSplitsInClient"));
    }
    otherArgs = cmdLine.getArgs();
    return defaultVal;
  }

}
