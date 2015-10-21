/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.analyzer.plugins;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.analyzer.Result;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.ATSImportTool;
import org.apache.tez.history.parser.ATSFileParser;
import org.apache.tez.history.parser.SimpleHistoryParser;
import org.apache.tez.history.parser.datamodel.DagInfo;

import com.google.common.base.Preconditions;

public abstract class TezAnalyzerBase extends Configured implements Tool, Analyzer {

  
  private static final String EVENT_FILE_NAME = "eventFileName";
  private static final String OUTPUT_DIR = "outputDir";
  private static final String SAVE_RESULTS = "saveResults";
  private static final String DAG_ID = "dagId";
  private static final String FROM_SIMPLE_HISTORY = "fromSimpleHistory";
  private static final String HELP = "help";

  private static final int SEPARATOR_WIDTH = 80;
  private static final int MIN_COL_WIDTH = 12;

  private String outputDir;
  private boolean saveResults = false;
  
  @SuppressWarnings("static-access")
  private static Options buildOptions() {
    Option dagIdOption = OptionBuilder.withArgName(DAG_ID).withLongOpt(DAG_ID)
        .withDescription("DagId that needs to be analyzed").hasArg().isRequired(true).create();

    Option outputDirOption = OptionBuilder.withArgName(OUTPUT_DIR).withLongOpt(OUTPUT_DIR)
        .withDescription("Directory to write outputs to.").hasArg().isRequired(false).create();

    Option saveResults = OptionBuilder.withArgName(SAVE_RESULTS).withLongOpt(SAVE_RESULTS)
        .withDescription("Saves results to output directory (optional)")
        .hasArg(false).isRequired(false).create();

    Option eventFileNameOption = OptionBuilder.withArgName(EVENT_FILE_NAME).withLongOpt
        (EVENT_FILE_NAME)
        .withDescription("File with event data for the DAG").hasArg()
        .isRequired(false).create();
    
    Option fromSimpleHistoryOption = OptionBuilder.withArgName(FROM_SIMPLE_HISTORY).withLongOpt
        (FROM_SIMPLE_HISTORY)
        .withDescription("Event data from Simple History logging. Must also specify event file")
        .isRequired(false).create();
    
    Option help = OptionBuilder.withArgName(HELP).withLongOpt
        (HELP)
        .withDescription("print help")
        .isRequired(false).create();

    Options opts = new Options();
    opts.addOption(dagIdOption);
    opts.addOption(outputDirOption);
    opts.addOption(saveResults);
    opts.addOption(eventFileNameOption);
    opts.addOption(fromSimpleHistoryOption);
    opts.addOption(help);
    return opts;
  }
  
  protected String getOutputDir() {
    return outputDir;
  }
  
  private void printUsage() {
    System.err.println("Analyzer base options are");
    Options options = buildOptions();
    for (Object obj : options.getOptions()) {
      Option option = (Option) obj;
      System.err.println(option.getArgName() + " : " + option.getDescription());
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    //Parse downloaded contents
    CommandLine cmdLine = null;
    try {
      cmdLine = new GnuParser().parse(buildOptions(), args);
    } catch (ParseException e) {
      System.err.println("Invalid options on command line");
      printUsage();
      return -1;
    }
    saveResults = cmdLine.hasOption(SAVE_RESULTS);
    
    if(cmdLine.hasOption(HELP)) {
      printUsage();
      return 0;
    }

    outputDir = cmdLine.getOptionValue(OUTPUT_DIR);
    if (outputDir == null) {
      outputDir = System.getProperty("user.dir");
    }

    File file = null;
    if (cmdLine.hasOption(EVENT_FILE_NAME)) {
      file = new File(cmdLine.getOptionValue(EVENT_FILE_NAME));
    }
    
    String dagId = cmdLine.getOptionValue(DAG_ID);
    
    DagInfo dagInfo = null;
    
    if (file == null) {
      if (cmdLine.hasOption(FROM_SIMPLE_HISTORY)) {
        System.err.println("Event file name must be specified when using simple history");
        printUsage();
        return -2;
      }
      // using ATS - try to download directly
      String[] importArgs = { "--dagId=" + dagId, "--downloadDir=" + outputDir };

      int result = ATSImportTool.process(importArgs);
      if (result != 0) {
        System.err.println("Error downloading data from ATS");
        return -3;
      }

      //Parse ATS data and verify results
      //Parse downloaded contents
      file = new File(outputDir
          + Path.SEPARATOR + dagId
          + Path.SEPARATOR + dagId + ".zip");
    }
    
    Preconditions.checkState(file != null);
    if (!cmdLine.hasOption(FROM_SIMPLE_HISTORY)) {
      ATSFileParser parser = new ATSFileParser(file);
      dagInfo = parser.getDAGData(dagId);
    } else {
      SimpleHistoryParser parser = new SimpleHistoryParser(file);
      dagInfo = parser.getDAGData(dagId);
    }
    Preconditions.checkState(dagInfo.getDagId().equals(dagId));
    analyze(dagInfo);
    Result result = getResult();
    if (saveResults && (result instanceof CSVResult)) {
      String fileName = outputDir + File.separator
          + this.getClass().getName() + "_" + dagInfo.getDagId() + ".csv";
      ((CSVResult) result).dumpToFile(fileName);
      System.out.println("Saved results in " + fileName);
    }
    return 0;
  }

  public void printResults() throws TezException {
    Result result = getResult();
    if (result instanceof CSVResult) {
      String[] headers = ((CSVResult) result).getHeaders();

      StringBuilder formatBuilder = new StringBuilder();
      int size = Math.max(MIN_COL_WIDTH, SEPARATOR_WIDTH / headers.length);
      for (int i = 0; i < headers.length; i++) {
        formatBuilder.append("%-").append(size).append("s ");
      }
      String format = formatBuilder.toString();

      StringBuilder separator = new StringBuilder();
      for (int i = 0; i < SEPARATOR_WIDTH; i++) {
        separator.append("-");
      }

      System.out.println(separator);
      System.out.println(String.format(format.toString(), (String[]) headers));
      System.out.println(separator);

      Iterator<String[]> recordsIterator = ((CSVResult) result).getRecordsIterator();
      while (recordsIterator.hasNext()) {
        String line = String.format(format, (String[]) recordsIterator.next());
        System.out.println(line);
      }
      System.out.println(separator);
    }
  }
}
