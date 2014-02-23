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

package org.apache.tez.runtime.library.input;

import java.io.IOException;

import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.library.api.KeyValuesReader;

public class ConcatenatedMergedKeyValuesInput extends MergedLogicalInput {

  public class ConcatenatedMergedKeyValuesReader implements KeyValuesReader {
    private int currentReaderIndex = 0;
    private KeyValuesReader currentReader;
    
    @Override
    public boolean next() throws IOException {
      while ((currentReader == null) || !currentReader.next()) {
        if (currentReaderIndex == getInputs().size()) {
          return false;
        }
        try {
          Reader reader = getInputs().get(currentReaderIndex).getReader();
          if (!(reader instanceof KeyValuesReader)) {
            throw new TezUncheckedException("Expected KeyValuesReader. "
                + "Got: " + reader.getClass().getName());
          }
          currentReader = (KeyValuesReader) reader;
          currentReaderIndex++;
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      return true;
    }

    @Override
    public Object getCurrentKey() throws IOException {
      return currentReader.getCurrentKey();
    }

    @Override
    public Iterable<Object> getCurrentValues() throws IOException {
      return currentReader.getCurrentValues();
    }
    
  }
    
  @Override
  public Reader getReader() throws Exception {
    return new ConcatenatedMergedKeyValuesReader();
  }

  @Override
  public void setConstituentInputIsReady(Input input) {
    informInputReady();
  }
}