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
package org.apache.tez.tools.webapp.controller;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.analyzer.plugins.TezAnalyzerBase;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.apache.tez.tools.webapp.service.AnalyzerService;
import org.apache.tez.tools.webapp.service.AnalyzerService.AnalyzerOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/analyzer")
public class AnalyzerController {
  private final Logger LOG = LoggerFactory.getLogger(getClass());

  private static final String HTML_CHECKBOX_PREFIX = "check_analyzer_";
  private static final String HTML_CONFIG_PREFIX = "config_analyzer_";
  private static final String PARAM_DAGID = "dagId";
  private static final String PARAM_DAGFILES = "dagFiles";
  private static final String PARAM_OUTPUT_FORMAT = "outputFormat";

  @Autowired
  private AnalyzerService analyzerService;

  /**
   * Loads the analyzer.html webpage and fills thymeleaf template with analyzer info.
   * @param model
   * @return the template file name to serve
   */
  @RequestMapping(method = RequestMethod.GET)
  public String getPage(Model model) {
    List<TezAnalyzerBase> analyzers = analyzerService.getAllAnalyzers();
    model.addAttribute("analyzers", analyzers);
    return "analyzer.html";
  }

  /**
   * Runs analyzers on a given dag.
   * @param request
   * @param response
   * @return a zip file containing output files of analyzers
   * @throws Exception
   */
  @RequestMapping(method = RequestMethod.POST, produces = "application/zip")
  public @ResponseBody byte[] analyze(HttpServletRequest request, HttpServletResponse response) throws Exception {
    List<File> files = ControllerUtils.getFiles(request, PARAM_DAGFILES);
    Configuration configuration = getConfiguration(request);
    List<TezAnalyzerBase> analyzers = getAnalyzers(request, configuration);
    DagInfo dagInfo = getDagInfo(request, files);
    AnalyzerOutputFormat aof = getOutputFormat(request);

    File zipFile = analyzerService.runAnalyzers(analyzers, dagInfo, aof);

    response.setHeader("Content-Disposition", "attachment; filename=\"" + zipFile.getName() + "\"");
    LOG.info("Finished analyzing dag: {}", dagInfo.getDagId());
    return Files.readAllBytes(zipFile.toPath());
  }

  private AnalyzerOutputFormat getOutputFormat(HttpServletRequest request) {
    String outputFormat = request.getParameter(PARAM_OUTPUT_FORMAT);
    if (outputFormat == null || outputFormat.isEmpty()) {
      outputFormat = "csv";
    }
    return AnalyzerOutputFormat.valueOf(outputFormat.toUpperCase());
  }

  private Configuration getConfiguration(HttpServletRequest request) {
    Configuration configuration = new Configuration();
    configuration.set("output-dir", System.getProperty("java.io.tmpdir"));

    for (String param : request.getParameterMap().keySet()) {
      if (param.startsWith(HTML_CONFIG_PREFIX) && !request.getParameter(param).isEmpty()) {
        LOG.info("Setting config parameter {} = {}", param.substring(HTML_CONFIG_PREFIX.length()),
            request.getParameter(param));
        configuration.set(param, request.getParameter(param));
      }
    }
    return configuration;
  }

  private List<TezAnalyzerBase> getAnalyzers(HttpServletRequest request, Configuration config) throws Exception {
    List<TezAnalyzerBase> analyzers = new ArrayList<>();
    for (String param : request.getParameterMap().keySet()) {
      if (param.startsWith(HTML_CHECKBOX_PREFIX)) {
        String className = AnalyzerService.ANALYZER_PACKAGE + "." + param.substring(HTML_CHECKBOX_PREFIX.length());
        analyzers
            .add((TezAnalyzerBase) Class.forName(className).getConstructor(Configuration.class).newInstance(config));
      }
    }
    return analyzers;
  }

  private DagInfo getDagInfo(HttpServletRequest request, List<File> files) throws Exception {
    String dagId = request.getParameter(PARAM_DAGID);
    return analyzerService.getDagInfo(dagId, files);
  }
}
