package org.apache.tez.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;

import com.google.common.base.Preconditions;

public class SumProcessor extends SimpleMRProcessor {
  public SumProcessor(ProcessorContext context) {
    super(context);
  }

  static String OUTPUT = "Output";
  static String TOKENIZER = "Tokenizer";

  @Override
  public void run() throws Exception {
    Preconditions.checkArgument(getInputs().size() == 1);
    Preconditions.checkArgument(getOutputs().size() == 1);
    KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
    // The KeyValues reader provides all values for a given key. The aggregation of values per key
    // is done by the LogicalInput. Since the key is the word and the values are its counts in
    // the different TokenProcessors, summing all values per key provides the sum for that word.
    KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(TOKENIZER).getReader();
    while (kvReader.next()) {
      Text word = (Text) kvReader.getCurrentKey();
      int sum = 0;
      for (Object value : kvReader.getCurrentValues()) {
        sum += ((IntWritable) value).get();
      }
      kvWriter.write(word, new IntWritable(sum));
    }
    // deriving from SimpleMRProcessor takes care of committing the output
    // It automatically invokes the commit logic for the OutputFormat if necessary.
  }
}
