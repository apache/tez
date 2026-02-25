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
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.tez.tools.webapp.service.EventDumpService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value = "/eventdump")
public class EventDumpController {
  private static final String PARAM_DAGID = "dagId";
  private static final String PARAM_DAGFILES = "dagFiles";

  @Autowired
  private EventDumpService eventDumpService;

  /**
   * Loads the eventdump.html webpage.
   * @param model
   * @return the template file name to serve
   */
  @RequestMapping(method = RequestMethod.GET)
  public String getPage(Model model) {
    return "eventdump.html";
  }

  /**
   * Dumps all events from an event file and returns a human-readable array of jsons.
   * @param request
   * @param response
   * @return an array of events as JSONObject
   * @throws Exception
   */
  @RequestMapping(method = RequestMethod.POST, produces = "application/json")
  public @ResponseBody String dump(HttpServletRequest request, HttpServletResponse response) throws Exception {
    List<File> files = ControllerUtils.getFiles(request, PARAM_DAGFILES);
    String dagId = request.getParameter(PARAM_DAGID);

    return eventDumpService.getArrayOfAllEvents(dagId, files).toString();
  }
}
