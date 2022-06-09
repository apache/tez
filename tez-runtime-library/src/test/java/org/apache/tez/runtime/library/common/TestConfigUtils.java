/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.common;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;

import static org.junit.Assert.assertEquals;

public class TestConfigUtils {

  private static class CustomKey implements WritableComparable<CustomKey>, Configurable {

    private Configuration conf;

    @Override
    public int compareTo(CustomKey o) {
      return 0;
    }

    @Override
    public void write(DataOutput out) {

    }

    @Override
    public void readFields(DataInput in) {

    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }
  }

  @Test
  public void getIntermediateOutputKeyComparator() {
    Configuration conf = new Configuration();
    String testKey = "test_flag_name";
    String testValue = "tez";
    conf.set(testKey, testValue);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, CustomKey.class.getName());
    WritableComparator rawComparator = (WritableComparator) ConfigUtils.getIntermediateOutputKeyComparator(conf);
    CustomKey customKey = (CustomKey) rawComparator.newKey();
    assertEquals(testValue, customKey.getConf().get(testKey));
  }

  @Test
  public void getIntermediateInputKeyComparator() {
    Configuration conf = new Configuration();
    String testKey = "test_flag_name";
    String testValue = "tez";
    conf.set(testKey, testValue);
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS, CustomKey.class.getName());
    WritableComparator rawComparator = (WritableComparator) ConfigUtils.getIntermediateInputKeyComparator(conf);
    CustomKey customKey = (CustomKey) rawComparator.newKey();
    assertEquals(testValue, customKey.getConf().get(testKey));
  }
}
