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
package org.apache.tez.common.web;

import com.google.common.base.Joiner;

import org.apache.hadoop.http.HttpServer2;
import org.apache.tez.common.TezUtilsInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <pre>
 * Servlet that runs async-profiler as web-endpoint.
 * Following options from async-profiler can be specified as query parameter.
 * //  -e event          profiling event: cpu|alloc|lock|cache-misses etc.
 * //  -d duration       run profiling for{@literal <duration>} seconds (integer)
 * //  -i interval       sampling interval in nanoseconds (long)
 * //  -j jstackdepth    maximum Java stack depth (integer)
 * //  -b bufsize        frame buffer size (long)
 * //  -m method         fully qualified method name: 'ClassName.methodName'
 * //  -t                profile different threads separately
 * //  -s                simple class names instead of FQN
 * //  -o fmt[,fmt...]   output format: summary|traces|flat|collapsed|svg|tree|jfr|html
 * //  --width px        SVG width pixels (integer)
 * //  --height px       SVG frame height pixels (integer)
 * //  --minwidth px     skip frames smaller than px (double)
 * //  --reverse         generate stack-reversed FlameGraph / Call tree
 * Example:
 * - To collect 10 second CPU profile of current process (returns FlameGraph html)
 * {@literal curl "http://localhost:10002/prof"}
 * - To collect 1 minute CPU profile of current process and output in tree format (html)
 * {@literal curl  "http://localhost:10002/prof?output=tree&duration=60"}
 * - To collect 10 second heap allocation profile of current process (returns FlameGraph html)
 * {@literal curl "http://localhost:10002/prof?event=alloc"}
 * - To collect lock contention profile of current process (returns FlameGraph html)
 * {@literal curl "http://localhost:10002/prof?event=lock"}
 * Following event types are supported (default is 'cpu') (NOTE: not all OS'es support all events)
 * // Perf events:
 * //    cpu
 * //    page-faults
 * //    context-switches
 * //    cycles
 * //    instructions
 * //    cache-references
 * //    cache-misses
 * //    branches
 * //    branch-misses
 * //    bus-cycles
 * //    L1-dcache-load-misses
 * //    LLC-load-misses
 * //    dTLB-load-misses
 * //    mem:breakpoint
 * //    trace:tracepoint
 * // Java events:
 * //    alloc
 * //    lock
 * </pre>
 */
public class ProfileServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ProfileServlet.class);
  private static final String ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
  private static final String ALLOWED_METHODS = "GET";
  private static final String ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
  private static final String CONTENT_TYPE_TEXT = "text/plain; charset=utf-8";
  private static final String ASYNC_PROFILER_HOME_ENV = "ASYNC_PROFILER_HOME";
  private static final String ASYNC_PROFILER_HOME_SYSTEM_PROPERTY = "async.profiler.home";
  private static final int DEFAULT_DURATION_SECONDS = 10;
  private static final AtomicInteger ID_GEN = new AtomicInteger(0);
  public static final String OUTPUT_DIR = System.getProperty("java.io.tmpdir") + "/prof-output";

  enum Event {
    CPU("cpu"),
    ALLOC("alloc"),
    LOCK("lock"),
    PAGE_FAULTS("page-faults"),
    CONTEXT_SWITCHES("context-switches"),
    CYCLES("cycles"),
    INSTRUCTIONS("instructions"),
    CACHE_REFERENCES("cache-references"),
    CACHE_MISSES("cache-misses"),
    BRANCHES("branches"),
    BRANCH_MISSES("branch-misses"),
    BUS_CYCLES("bus-cycles"),
    L1_DCACHE_LOAD_MISSES("L1-dcache-load-misses"),
    LLC_LOAD_MISSES("LLC-load-misses"),
    DTLB_LOAD_MISSES("dTLB-load-misses"),
    MEM_BREAKPOINT("mem:breakpoint"),
    TRACE_TRACEPOINT("trace:tracepoint");

    private final String internalName;

    Event(final String internalName) {
      this.internalName = internalName;
    }

    public String getInternalName() {
      return internalName;
    }

    public static Event fromInternalName(final String name) {
      for (Event event : values()) {
        if (event.getInternalName().equalsIgnoreCase(name)) {
          return event;
        }
      }
      return null;
    }
  }

  enum Output {
    SUMMARY, TRACES, FLAT, COLLAPSED, SVG, TREE, JFR, HTML
  }

  private final Lock profilerLock = new ReentrantLock();
  private Integer pid;
  private String asyncProfilerHome;
  private transient Process process;

  public ProfileServlet() {
    this.asyncProfilerHome = getAsyncProfilerHome();
    this.pid = TezUtilsInternal.getPid();
    LOG.info("Servlet process PID: {} asyncProfilerHome: {}", pid, asyncProfilerHome);
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    response.setContentType("text/plain; charset=UTF-8");
    PrintStream out = new PrintStream(response.getOutputStream(), false, "UTF-8");
    if (!HttpServer2.isInstrumentationAccessAllowed(this.getServletContext(), request, response)) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      setResponseHeader(response);
      out.println("Unauthorized: Instrumentation access is not allowed!");
      out.close();
      return;
    }

    // make sure async profiler home is set
    if (asyncProfilerHome == null || asyncProfilerHome.trim().isEmpty()) {
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      setResponseHeader(response);
      out.println("ASYNC_PROFILER_HOME env is not set");
      out.close();
      return;
    }

    // if pid is explicitly specified, use it else default to current process
    pid = getInteger(request, "pid", pid);
    // if pid is not specified in query param and if current process pid cannot be determined
    if (pid == null) {
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      setResponseHeader(response);
      out.println("'pid' query parameter unspecified or unable to determine PID of current process.");
      out.close();
      return;
    }

    final int duration = getInteger(request, "duration", DEFAULT_DURATION_SECONDS);
    final Output output = getOutput(request);
    final Event event = getEvent(request);
    final Long interval = getLong(request, "interval");
    final Integer jstackDepth = getInteger(request, "jstackdepth", null);
    final Long bufsize = getLong(request, "bufsize");
    final boolean thread = request.getParameterMap().containsKey("thread");
    final boolean simple = request.getParameterMap().containsKey("simple");
    final Integer width = getInteger(request, "width", null);
    final Integer height = getInteger(request, "height", null);
    final Double minwidth = getMinWidth(request);
    final boolean reverse = request.getParameterMap().containsKey("reverse");
    final String method = request.getParameter("method");

    if (request.getParameter("event") != null && method != null) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      setResponseHeader(response);
      response.getWriter().write("Event and method aren't allowed to be both used in the same request.");
      return;
    }

    if (process == null || !process.isAlive()) {
      try {
        int lockTimeoutSecs = 3;
        if (profilerLock.tryLock(lockTimeoutSecs, TimeUnit.SECONDS)) {
          try {
            File outputFile = new File(OUTPUT_DIR,
                "async-prof-pid-" + pid + "-"
                    + (method == null ? event.name().toLowerCase() : method) + "-" + ID_GEN.incrementAndGet() + "."
                    + output.name().toLowerCase());
            List<String> cmd = new ArrayList<>();
            cmd.add(getProfilerScriptPath());
            cmd.add("-e");
            cmd.add(method == null ? event.getInternalName() : method);
            cmd.add("-d");
            cmd.add("" + duration);
            cmd.add("-o");
            cmd.add(output.name().toLowerCase());
            cmd.add("-f");
            cmd.add(outputFile.getAbsolutePath());
            if (interval != null) {
              cmd.add("-i");
              cmd.add(interval.toString());
            }
            if (jstackDepth != null) {
              cmd.add("-j");
              cmd.add(jstackDepth.toString());
            }
            if (bufsize != null) {
              cmd.add("-b");
              cmd.add(bufsize.toString());
            }
            if (thread) {
              cmd.add("-t");
            }
            if (simple) {
              cmd.add("-s");
            }
            if (width != null) {
              cmd.add("--width");
              cmd.add(width.toString());
            }
            if (height != null) {
              cmd.add("--height");
              cmd.add(height.toString());
            }
            if (minwidth != null) {
              cmd.add("--minwidth");
              cmd.add(minwidth.toString());
            }
            if (reverse) {
              cmd.add("--reverse");
            }
            cmd.add(pid.toString());
            process = new ProcessBuilder(cmd).start();

            // set response and set refresh header to output location
            setResponseHeader(response);
            response.setStatus(HttpServletResponse.SC_ACCEPTED);
            String relativeUrl = "/prof-output";
            // to avoid auto-refresh by ProfileOutputServlet, refreshDelay can be specified via url param
            int refreshDelay = getInteger(request, "refreshDelay", 0);
            // instead of sending redirect, set auto-refresh so that browsers will refresh with redirected url
            response.setHeader("Refresh", (duration + refreshDelay) + "; URL=" + relativeUrl + '?'
                + ProfileOutputServlet.FILE_QUERY_PARAM + '=' + outputFile.getName());

            out.println("Profiled PID: " + pid);
            out.println("Started [" + event.getInternalName()
                + "] profiling. This page will automatically redirect to "
                + relativeUrl + " after " + duration + " seconds.\n\ncommand:\n" + Joiner.on(" ").join(cmd));
            out.flush();
          } finally {
            profilerLock.unlock();
          }
        } else {
          setResponseHeader(response);
          response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
          out.println("Unable to acquire lock. Another instance of profiler might be running.");
          LOG.warn("Unable to acquire lock in {} seconds. Another instance of profiler might be running.",
              lockTimeoutSecs);
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while acquiring profile lock.", e);
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    } else {
      setResponseHeader(response);
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      out.println("Another instance of profiler is already running.");
    }
    out.close();
  }

  /**
   * Get the path of the profiler script to be executed.
   * Before async-profiler 3.0, the script was named profiler.sh, and after 3.0 it's bin/asprof
   * @return
   */
  private String getProfilerScriptPath() {
    Path defaultPath = Paths.get(asyncProfilerHome + "/bin/asprof");
    return Files.exists(defaultPath)? defaultPath.toString() : asyncProfilerHome + "/profiler.sh";
  }

  private Integer getInteger(final HttpServletRequest req, final String param, final Integer defaultValue) {
    final String value = req.getParameter(param);
    if (value != null) {
      try {
        return Integer.valueOf(value);
      } catch (NumberFormatException e) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  private Long getLong(final HttpServletRequest req, final String param) {
    final String value = req.getParameter(param);
    if (value != null) {
      try {
        return Long.valueOf(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private Double getMinWidth(final HttpServletRequest req) {
    final String value = req.getParameter("minwidth");
    if (value != null) {
      try {
        return Double.valueOf(value);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  private Event getEvent(final HttpServletRequest req) {
    final String eventArg = req.getParameter("event");
    if (eventArg != null) {
      Event event = Event.fromInternalName(eventArg);
      return event == null ? Event.CPU : event;
    }
    return Event.CPU;
  }

  private Output getOutput(final HttpServletRequest req) {
    final String outputArg = req.getParameter("output");
    if (outputArg != null) {
      try {
        return Output.valueOf(outputArg.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        LOG.warn("Output format value is invalid, returning with default HTML");
        return Output.HTML;
      }
    }
    return Output.HTML;
  }

  private void setResponseHeader(final HttpServletResponse response) {
    response.setHeader(ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
    response.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, "*");
    response.setContentType(CONTENT_TYPE_TEXT);
  }

  public static String getAsyncProfilerHome() {
    String asyncProfilerHome = System.getenv(ASYNC_PROFILER_HOME_ENV);
    // if ENV is not set, see if -Dasync.profiler.home=/path/to/async/profiler/home is set
    if (asyncProfilerHome == null || asyncProfilerHome.trim().isEmpty()) {
      asyncProfilerHome = System.getProperty(ASYNC_PROFILER_HOME_SYSTEM_PROPERTY);
    }
    return asyncProfilerHome;
  }
}
