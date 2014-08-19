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

package org.apache.hadoop.mapred.split;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.split.TezMapReduceSplitsGrouper;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

import static org.mockito.Mockito.*;

public class TestGroupedSplits {
  private static final Log LOG =
    LogFactory.getLog(TestGroupedSplits.class);

  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null;

  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  @SuppressWarnings("deprecation")
  private static Path workDir =
    new Path(new Path(System.getProperty("test.build.data", "/tmp")),
             "TestCombineTextInputFormat").makeQualified(localFs);

  // A reporter that does nothing
  private static final Reporter voidReporter = Reporter.NULL;

  //@Test(timeout=10000)
  public void testFormat() throws Exception {
    JobConf job = new JobConf(defaultConf);

    Random random = new Random();
    long seed = random.nextLong();
    LOG.info("seed = "+seed);
    random.setSeed(seed);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir);

    final int length = 10000;
    final int numFiles = 10;

    createFiles(length, numFiles, random);

    // create a combined split for the files
    TextInputFormat wrappedFormat = new TextInputFormat();
    wrappedFormat.configure(job);
    TezGroupedSplitsInputFormat<LongWritable , Text> format = 
        new TezGroupedSplitsInputFormat<LongWritable, Text>();
    format.setConf(job);
    format.setDesiredNumberOfSplits(1);
    format.setInputFormat(wrappedFormat);
    LongWritable key = new LongWritable();
    Text value = new Text();
    for (int i = 0; i < 3; i++) {
      int numSplits = random.nextInt(length/20)+1;
      LOG.info("splitting: requesting = " + numSplits);
      InputSplit[] splits = format.getSplits(job, numSplits);
      LOG.info("splitting: got =        " + splits.length);

      // we should have a single split as the length is comfortably smaller than
      // the block size
      Assert.assertEquals("We got more than one splits!", 1, splits.length);
      InputSplit split = splits[0];
      Assert.assertEquals("It should be TezGroupedSplit",
        TezGroupedSplit.class, split.getClass());

      // check the split
      BitSet bits = new BitSet(length);
      LOG.debug("split= " + split);
      RecordReader<LongWritable, Text> reader =
        format.getRecordReader(split, job, voidReporter);
      try {
        int count = 0;
        while (reader.next(key, value)) {
          int v = Integer.parseInt(value.toString());
          LOG.debug("read " + v);
          if (bits.get(v)) {
            LOG.warn("conflict with " + v +
                     " at position "+reader.getPos());
          }
          Assert.assertFalse("Key in multiple partitions.", bits.get(v));
          bits.set(v);
          count++;
        }
        LOG.info("splits="+split+" count=" + count);
      } finally {
        reader.close();
      }
      Assert.assertEquals("Some keys in no partition.", length, bits.cardinality());
    }
  }

  private static class Range {
    private final int start;
    private final int end;

    Range(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public String toString() {
      return "(" + start + ", " + end + ")";
    }
  }

  private static Range[] createRanges(int length, int numFiles, Random random) {
    // generate a number of files with various lengths
    Range[] ranges = new Range[numFiles];
    for (int i = 0; i < numFiles; i++) {
      int start = i == 0 ? 0 : ranges[i-1].end;
      int end = i == numFiles - 1 ?
        length :
        (length/numFiles)*(2*i + 1)/2 + random.nextInt(length/numFiles) + 1;
      ranges[i] = new Range(start, end);
    }
    return ranges;
  }

  private static void createFiles(int length, int numFiles, Random random)
    throws IOException {
    Range[] ranges = createRanges(length, numFiles, random);

    for (int i = 0; i < numFiles; i++) {
      Path file = new Path(workDir, "test_" + i + ".txt");
      Writer writer = new OutputStreamWriter(localFs.create(file));
      Range range = ranges[i];
      try {
        for (int j = range.start; j < range.end; j++) {
          writer.write(Integer.toString(j));
          writer.write("\n");
        }
      } finally {
        writer.close();
      }
    }
  }

  private static void writeFile(FileSystem fs, Path name,
                                CompressionCodec codec,
                                String contents) throws IOException {
    OutputStream stm;
    if (codec == null) {
      stm = fs.create(name);
    } else {
      stm = codec.createOutputStream(fs.create(name));
    }
    stm.write(contents.getBytes());
    stm.close();
  }

  private static List<Text> readSplit(InputFormat<LongWritable,Text> format,
                                      InputSplit split,
                                      JobConf job) throws IOException {
    List<Text> result = new ArrayList<Text>();
    RecordReader<LongWritable, Text> reader =
      format.getRecordReader(split, job, voidReporter);
    LongWritable key = reader.createKey();
    Text value = reader.createValue();
    while (reader.next(key, value)) {
      result.add(value);
      value = reader.createValue();
    }
    reader.close();
    return result;
  }

  /**
   * Test using the gzip codec for reading
   */
  //@Test(timeout=10000)
  public void testGzip() throws IOException {
    JobConf job = new JobConf(defaultConf);
    CompressionCodec gzip = new GzipCodec();
    ReflectionUtils.setConf(gzip, job);
    localFs.delete(workDir, true);
    writeFile(localFs, new Path(workDir, "part1.txt.gz"), gzip,
              "the quick\nbrown\nfox jumped\nover\n the lazy\n dog\n");
    writeFile(localFs, new Path(workDir, "part2.txt.gz"), gzip,
              "is\ngzip\n");
    writeFile(localFs, new Path(workDir, "part3.txt.gz"), gzip,
        "one\nmore\nsplit\n");
    FileInputFormat.setInputPaths(job, workDir);
    TextInputFormat wrappedFormat = new TextInputFormat();
    wrappedFormat.configure(job);
    TezGroupedSplitsInputFormat<LongWritable , Text> format = 
        new TezGroupedSplitsInputFormat<LongWritable, Text>();
    format.setConf(job);
    format.setInputFormat(wrappedFormat);
    
    // TextInputFormat will produce 3 splits
    for (int j=1; j<=3; ++j) {
      format.setDesiredNumberOfSplits(j);
      InputSplit[] splits = format.getSplits(job, 100);
      if (j==1 || j==3) {
        // j==1 covers single split corner case
        // j==3 cases exercises the code where desired == actual
        // and does not do grouping
        Assert.assertEquals("compressed splits == " + j, j, splits.length);
      }
      List<Text> results = new ArrayList<Text>();
      for (int i=0; i<splits.length; ++i) { 
        List<Text> read = readSplit(format, splits[i], job);
        results.addAll(read);
      }
      Assert.assertEquals("splits length", 11, results.size());
  
      final String[] firstList =
        {"the quick", "brown", "fox jumped", "over", " the lazy", " dog"};
      final String[] secondList = {"is", "gzip"};
      final String[] thirdList = {"one", "more", "split"};
      String first = results.get(0).toString();
      int start = 0;
      switch (first.charAt(0)) {
      case 't':
        start = testResults(results, firstList, start);
        break;
      case 'i':
        start = testResults(results, secondList, start);
        break;
      case 'o':
        start = testResults(results, thirdList, start);
        break;
      default:
        Assert.fail("unexpected first token - " + first);
      }
    }
  }

  private static int testResults(List<Text> results, String[] first, int start) {
    for (int i = 0; i < first.length; i++) {
      Assert.assertEquals("splits["+i+"]", first[i], results.get(start+i).toString());
    }
    return first.length+start;
  }  
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test(timeout=10000)
  public void testGroupedSplitSize() throws IOException {
    JobConf job = new JobConf(defaultConf);
    InputFormat mockWrappedFormat = mock(InputFormat.class);
    TezGroupedSplitsInputFormat<LongWritable , Text> format = 
        new TezGroupedSplitsInputFormat<LongWritable, Text>();
    format.setConf(job);
    format.setInputFormat(mockWrappedFormat);
    
    job = (JobConf) TezMapReduceSplitsGrouper.createConfigBuilder(job)
        .setGroupingSplitSize(50*1000*1000l, 500*1000*1000l)
        .build();
    InputSplit mockSplit1 = mock(InputSplit.class);
    when(mockSplit1.getLength()).thenReturn(10*1000*1000l);
    when(mockSplit1.getLocations()).thenReturn(null);
    int numSplits = 100;
    InputSplit[] mockSplits = new InputSplit[numSplits];
    for (int i=0; i<numSplits; i++) {
      mockSplits[i] = mockSplit1;
    }
    when(mockWrappedFormat.getSplits((JobConf)anyObject(), anyInt())).thenReturn(mockSplits);
    
    // desired splits not set. We end up choosing min/max split size based on 
    // total data and num original splits. In this case, min size will be hit
    InputSplit[] splits = format.getSplits(job, 0);
    Assert.assertEquals(25, splits.length);
    
    // split too big. override with max
    format.setDesiredNumberOfSplits(1);
    splits = format.getSplits(job, 0);
    Assert.assertEquals(4, splits.length);
    
    // splits too small. override with min
    format.setDesiredNumberOfSplits(1000);
    splits = format.getSplits(job, 0);
    Assert.assertEquals(25, splits.length);
    
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test(timeout=10000)
  public void testGroupedSplitWithDuplicates() throws IOException {
    JobConf job = new JobConf(defaultConf);
    InputFormat mockWrappedFormat = mock(InputFormat.class);
    TezGroupedSplitsInputFormat<LongWritable , Text> format = 
        new TezGroupedSplitsInputFormat<LongWritable, Text>();
    format.setConf(job);
    format.setInputFormat(mockWrappedFormat);
    
    // put multiple splits with multiple copies in the same location
    String[] locations = {"common", "common", "common"};
    int numSplits = 3;
    InputSplit[] mockSplits = new InputSplit[numSplits];
    for (int i=0; i<numSplits; i++) {
      InputSplit mockSplit = mock(InputSplit.class);
      when(mockSplit.getLength()).thenReturn(10*1000*1000l);
      when(mockSplit.getLocations()).thenReturn(locations);
      mockSplits[i] = mockSplit;
    }
    when(mockWrappedFormat.getSplits((JobConf)anyObject(), anyInt())).thenReturn(mockSplits);
    
    format.setDesiredNumberOfSplits(1);
    InputSplit[] splits = format.getSplits(job, 1);
    Assert.assertEquals(1, splits.length);
    TezGroupedSplit split = (TezGroupedSplit) splits[0];
    // all 3 splits are present
    Assert.assertEquals(numSplits, split.wrappedSplits.size());
    Set<InputSplit> splitSet = Sets.newHashSet(split.wrappedSplits);
    Assert.assertEquals(numSplits, splitSet.size());
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test(timeout=10000)
  public void testGroupedSplitWithBadLocations() throws IOException {
    JobConf job = new JobConf(defaultConf);
    InputFormat mockWrappedFormat = mock(InputFormat.class);
    TezGroupedSplitsInputFormat<LongWritable , Text> format = 
        new TezGroupedSplitsInputFormat<LongWritable, Text>();
    format.setConf(job);
    format.setInputFormat(mockWrappedFormat);
    
    // put multiple splits with multiple copies in the same location
    int numSplits = 3;
    InputSplit[] mockSplits = new InputSplit[numSplits];
    InputSplit mockSplit1 = mock(InputSplit.class);
    when(mockSplit1.getLength()).thenReturn(10*1000*1000l);
    when(mockSplit1.getLocations()).thenReturn(null);
    mockSplits[0] = mockSplit1;
    InputSplit mockSplit2 = mock(InputSplit.class);
    when(mockSplit2.getLength()).thenReturn(10*1000*1000l);
    when(mockSplit2.getLocations()).thenReturn(new String[] {null});
    mockSplits[1] = mockSplit2;
    InputSplit mockSplit3 = mock(InputSplit.class);
    when(mockSplit3.getLength()).thenReturn(10*1000*1000l);
    when(mockSplit3.getLocations()).thenReturn(new String[] {null, null});
    mockSplits[2] = mockSplit3;

    when(mockWrappedFormat.getSplits((JobConf)anyObject(), anyInt())).thenReturn(mockSplits);
    
    format.setDesiredNumberOfSplits(1);
    InputSplit[] splits = format.getSplits(job, 1);
    Assert.assertEquals(1, splits.length);
    TezGroupedSplit split = (TezGroupedSplit) splits[0];
    // all 3 splits are present
    Assert.assertEquals(numSplits, split.wrappedSplits.size());
    ByteArrayOutputStream bOut = new ByteArrayOutputStream();
    split.write(new DataOutputStream(bOut));
  }

}
