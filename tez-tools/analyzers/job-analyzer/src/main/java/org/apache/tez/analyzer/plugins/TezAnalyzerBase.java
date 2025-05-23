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
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.tez.analyzer.Analyzer;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.analyzer.Result;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.ATSImportTool;
import org.apache.tez.history.parser.ATSFileParser;
import org.apache.tez.history.parser.ProtoHistoryParser;
import org.apache.tez.history.parser.SimpleHistoryParser;
import org.apache.tez.history.parser.datamodel.DagInfo;

import org.apache.tez.common.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TezAnalyzerBase extends Configured implements Tool, Analyzer {

  private static final Logger LOG = LoggerFactory.getLogger(TezAnalyzerBase.class);
  
  private static final String EVENT_FILE_NAME = "eventFileName";
  private static final String OUTPUT_DIR = "outputDir";
  private static final String SAVE_RESULTS = "saveResults";
  private static final String DAG_ID = "dagId";
  private static final String FROM_SIMPLE_HISTORY = "fromSimpleHistory";
  private static final String FROM_PROTO_HISTORY = "fromProtoHistory";
  private static final String HELP = "help";

  private static final int SEPARATOR_WIDTH = 80;
  private static final int MIN_COL_WIDTH = 12;

  private String outputDir;
  private boolean saveResults = false;

  public TezAnalyzerBase(Configuration config) {
    setConf(config);
  }

  @SuppressWarnings("static-access")
  private static Options buildOptions() {
    Option dagIdOption = Option.builder().argName(DAG_ID).longOpt(DAG_ID)
        .desc("DagId that needs to be analyzed").hasArg().required(true).build();

    Option outputDirOption = Option.builder().argName(OUTPUT_DIR).longOpt(OUTPUT_DIR)
        .desc("Directory to write outputs to.").hasArg().required(false).build();

    Option saveResults = Option.builder().argName(SAVE_RESULTS).longOpt(SAVE_RESULTS)
        .desc("Saves results to output directory (optional)")
        .hasArg(false).required(false).build();

    Option eventFileNameOption = Option.builder().argName(EVENT_FILE_NAME).longOpt
        (EVENT_FILE_NAME)
        .desc("File with event data for the DAG").hasArg()
        .required(false).build();
    
    Option fromSimpleHistoryOption = Option.builder().argName(FROM_SIMPLE_HISTORY).longOpt
        (FROM_SIMPLE_HISTORY)
        .desc("Event data from Simple History logging. Must also specify event file")
        .required(false).build();

    Option fromProtoHistoryOption =
        Option.builder().argName(FROM_PROTO_HISTORY).longOpt(FROM_PROTO_HISTORY)
            .desc("Event data from Proto History logging. Must also specify event file")
            .required(false).build();

    Option help = Option.builder().argName(HELP).longOpt
        (HELP)
        .desc("print help")
        .required(false).build();

    Options opts = new Options();
    opts.addOption(dagIdOption);
    opts.addOption(outputDirOption);
    opts.addOption(saveResults);
    opts.addOption(eventFileNameOption);
    opts.addOption(fromSimpleHistoryOption);
    opts.addOption(fromProtoHistoryOption);
    opts.addOption(help);
    return opts;
  }
  
  protected String getOutputDir() {
    return outputDir;
  }
  
  private void printUsage() {
    System.err.println("Analyzer base options are");
    Options options = buildOptions();
    for (Option option : options.getOptions()) {
      System.err.println(option.getArgName() + " : " + option.getDescription());
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    //Parse downloaded contents
    CommandLine cmdLine = null;
    try {
      cmdLine = new DefaultParser().parse(buildOptions(), args);
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

    String dagId = cmdLine.getOptionValue(DAG_ID);

    List<File> files = new ArrayList<File>();
    if (cmdLine.hasOption(EVENT_FILE_NAME)) {
      for (String file : cmdLine.getOptionValue(EVENT_FILE_NAME).split(",")) {
        File fileOrDir = new File(file);
        if (fileOrDir.exists()) {
          if (fileOrDir.isFile()) {
            files.add(fileOrDir);
          } else {
            files.addAll(collectFilesForDagId(fileOrDir, dagId));
          }
        }
      }
    }

    DagInfo dagInfo = null;
    
    if (files.isEmpty()) {
      if (cmdLine.hasOption(FROM_SIMPLE_HISTORY)) {
        System.err.println("Event file name must be specified when using simple history");
        printUsage();
        return -2;
      }
      if (cmdLine.hasOption(FROM_PROTO_HISTORY)) {
        System.err.println("Proto file name must be specified when using proto history");
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
      files.add(new File(outputDir
          + Path.SEPARATOR + dagId + ".zip"));
    }
    
    Preconditions.checkState(!files.isEmpty());
    if (cmdLine.hasOption(FROM_SIMPLE_HISTORY)) {
      SimpleHistoryParser parser = new SimpleHistoryParser(files);
      dagInfo = parser.getDAGData(dagId);
    } else if (cmdLine.hasOption(FROM_PROTO_HISTORY)) {
      ProtoHistoryParser parser = new ProtoHistoryParser(files);
      dagInfo = parser.getDAGData(dagId);
    } else {
      ATSFileParser parser = new ATSFileParser(files);
      dagInfo = parser.getDAGData(dagId);
    }
    Preconditions.checkState(dagInfo.getDagId().equals(dagId));
    analyze(dagInfo);
    Result result = getResult();

    if (saveResults) {
      String dagInfoFileName = outputDir + File.separator + this.getClass().getName() + "_"
          + dagInfo.getDagId() + ".dag";
      FileUtils.writeStringToFile(new File(dagInfoFileName), dagInfo.toExtendedString());
      LOG.info("Saved dag info in " + dagInfoFileName);

      if (result instanceof CSVResult) {
        String fileName = outputDir + File.separator + this.getClass().getName() + "_"
            + dagInfo.getDagId() + ".csv";
        ((CSVResult) result).dumpToFile(fileName);
        LOG.info("Saved results in " + fileName);
      }
    }

    return 0;
  }

  private List<File> collectFilesForDagId(File parentDir, String dagId) {
    File[] arrFiles = parentDir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.contains(dagId);
      }
    });
    if (arrFiles == null || arrFiles.length == 0) {
      throw new RuntimeException(
          String.format("cannot find relevant files for dag: '%s' in dir: %s", dagId, parentDir));
    }

    List<File> files = Arrays.asList(arrFiles);
    LOG.info("collected files for dag: \n"
        + files.stream().map(f -> "\n" + f.getAbsolutePath()).collect(Collectors.toList()));
    return files;
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

      StringBuilder sep = new StringBuilder();
      for (int i = 0; i < SEPARATOR_WIDTH; i++) {
        sep.append("-");
      }

      String separator = sep.toString();
      LOG.debug(separator);
      LOG.debug(String.format(format.toString(), (String[]) headers));
      LOG.debug(separator);

      Iterator<String[]> recordsIterator = ((CSVResult) result).getRecordsIterator();
      while (recordsIterator.hasNext()) {
        String line = String.format(format, (String[]) recordsIterator.next());
        LOG.debug(line);
      }
      LOG.debug(separator);
    }
  }
}
