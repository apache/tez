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
package org.apache.tez.tools.webapp.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.tez.analyzer.CSVResult;
import org.apache.tez.analyzer.Result;
import org.apache.tez.analyzer.plugins.TezAnalyzerBase;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.history.parser.ATSFileParser;
import org.apache.tez.history.parser.ProtoHistoryParser;
import org.apache.tez.history.parser.SimpleHistoryParser;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.io.Files;

@Service
public class AnalyzerService {
  public enum AnalyzerOutputFormat{
    CSV, XLSX
  }

  public static final String ANALYZER_PACKAGE = "org.apache.tez.analyzer.plugins";

  private final Logger LOG = LoggerFactory.getLogger(getClass());

  private List<TezAnalyzerBase> analyzers;

  public List<TezAnalyzerBase> getAllAnalyzers() {
    if (this.analyzers == null) {
      this.analyzers = new ArrayList<>();

      Reflections reflections = new Reflections("org.apache.tez");
      Set<Class<? extends TezAnalyzerBase>> subTypes = reflections.getSubTypesOf(TezAnalyzerBase.class);

      Configuration configuration = new Configuration();
      subTypes.forEach(c -> {
        try {
          TezAnalyzerBase analyzer = c.getConstructor(Configuration.class).newInstance(configuration);
          analyzers.add(analyzer);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      Collections.sort(analyzers, new Comparator<TezAnalyzerBase>() {
        @Override
        public int compare(TezAnalyzerBase arg0, TezAnalyzerBase arg1) {
          return arg0.getClass().getSimpleName().compareTo(arg1.getClass().getSimpleName());
        }
      });
    }
    return this.analyzers;
  }

  public DagInfo getDagInfo(String dagId, List<File> files) throws Exception {
    LOG.info("Parsing history file for dag: {}", dagId);

    // .zip file is typically an ATS history file, let's try
    if ("zip".equals(Files.getFileExtension(files.get(0).getName()))) {
      LOG.info("Assuming ATS history file: {}", files.get(0));
      ATSFileParser parser = new ATSFileParser(files);
      return parser.getDAGData(dagId);
    } else {
      try {
        LOG.info("Trying as proto history files: {}", files);
        ProtoHistoryParser parser = new ProtoHistoryParser(files);
        return parser.getDAGData(dagId);
      } catch (TezException e) {
        if (e.getCause() instanceof IOException && e.getCause().getMessage().contains("not a SequenceFile")) {
          LOG.info("Trying as simple history files: {}", files);
          SimpleHistoryParser parser = new SimpleHistoryParser(files);
          return parser.getDAGData(dagId);
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public File runAnalyzers(List<TezAnalyzerBase> analyzers, DagInfo dagInfo, AnalyzerOutputFormat aof)
      throws FileNotFoundException, TezException, IOException, Exception {
    File zipFile = new File(System.getProperty("java.io.tmpdir"),
        String.format("dag_analyzer_result_%d_%s.zip", System.currentTimeMillis(), dagInfo.getDagId()));
    FileOutputStream fos = new FileOutputStream(zipFile);
    ZipOutputStream zipOut = new ZipOutputStream(fos);

    for (TezAnalyzerBase analyzer : analyzers) {
      LOG.info("Starting {} for dag: {}", analyzer.getClass().getSimpleName(), dagInfo.getDagId());

      analyzer.analyze(dagInfo);

      Result result = analyzer.getResult();
      if (result instanceof CSVResult) {
        String filePath = System.getProperty("java.io.tmpdir") + File.separator + analyzer.getClass().getSimpleName()
            + "_" + dagInfo.getDagId() + ".csv";
        ((CSVResult) result).dumpToFile(filePath);

        if (aof.equals(AnalyzerOutputFormat.XLSX)) {
          // TODO: convert directly to excel instead of writing csv file first
          // (not a prio, analyzers don't create huge files)
          filePath = convertCSVFileToExcel(filePath);
        }
        addFileToZip(new File(filePath), zipOut);
      }
    }
    zipOut.close();
    fos.close();
    return zipFile;
  }

  private String convertCSVFileToExcel(String filePath) throws IOException {
    try (XSSFWorkbook workBook = new XSSFWorkbook()) {
      Sheet sheet = workBook.createSheet(Paths.get(filePath).getFileName().toString());
      String currentLine = null;
      int rowNum = 0;
      try (BufferedReader br = java.nio.file.Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8)) {
        while ((currentLine = br.readLine()) != null) {
          String[] str = currentLine.split(",");
          rowNum++;
          Row currentRow = sheet.createRow(rowNum);
          for (int i = 0; i < str.length; i++) {
            currentRow.createCell(i).setCellValue(str[i]);
          }
        }
      }
      autoSizeColumns(workBook);
      String xlsxFilePath = filePath.substring(0, filePath.lastIndexOf(".csv")) + ".xlsx";
      FileOutputStream fileOutputStream = new FileOutputStream(xlsxFilePath);
      workBook.write(fileOutputStream);
      fileOutputStream.close();
      return xlsxFilePath;
    }
  }

  private void autoSizeColumns(Workbook workbook) {
    int numberOfSheets = workbook.getNumberOfSheets();
    for (int i = 0; i < numberOfSheets; i++) {
      Sheet sheet = workbook.getSheetAt(i);
      if (sheet.getPhysicalNumberOfRows() > 0) {
        Row row = sheet.getRow(sheet.getFirstRowNum());
        Iterator<Cell> cellIterator = row.cellIterator();
        while (cellIterator.hasNext()) {
          Cell cell = cellIterator.next();
          int columnIndex = cell.getColumnIndex();
          int originalColumnWidth = sheet.getColumnWidth(columnIndex);
          sheet.autoSizeColumn(columnIndex);
          if (sheet.getColumnWidth(columnIndex) > 20000) {
            // don't let e.g. comment columns expand too large, 20000 was picked by manual experience
            sheet.setColumnWidth(columnIndex, originalColumnWidth);
          }
        }
      }
    }
  }

  private void addFileToZip(File fileToZip, ZipOutputStream zipOut) throws Exception {
    try (FileInputStream fis = new FileInputStream(fileToZip)) {
      ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
      zipOut.putNextEntry(zipEntry);

      byte[] bytes = new byte[1024];
      int length;
      while ((length = fis.read(bytes)) >= 0) {
        zipOut.write(bytes, 0, length);
      }
    }
  }
}
