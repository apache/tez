/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.output;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads;
import org.junit.Test;

// Tests which don't require parameterization
public class TestUnorderedPartitionedKVOutput2 {


  @Test(timeout = 5000)
  public void testNonStartedOutput() throws Exception {
    OutputContext outputContext = OutputTestHelpers.createOutputContext();
    int numPartitions = 1;
    UnorderedPartitionedKVOutput output =
        new UnorderedPartitionedKVOutput(outputContext, numPartitions);
    output.initialize();
    List<Event> events = output.close();
    assertEquals(1, events.size());
    Event event1 = events.get(0);
    assertTrue(event1 instanceof CompositeDataMovementEvent);
    CompositeDataMovementEvent dme = (CompositeDataMovementEvent) event1;
    ByteBuffer bb = dme.getUserPayload();
    ShuffleUserPayloads.DataMovementEventPayloadProto shufflePayload =
        ShuffleUserPayloads.DataMovementEventPayloadProto.parseFrom(ByteString.copyFrom(bb));
    assertTrue(shufflePayload.hasEmptyPartitions());

    byte[] emptyPartitions = TezCommonUtils.decompressByteStringToByteArray(shufflePayload
        .getEmptyPartitions());
    BitSet emptyPartionsBitSet = TezUtilsInternal.fromByteArray(emptyPartitions);
    assertEquals(numPartitions, emptyPartionsBitSet.cardinality());
    for (int i = 0; i < numPartitions; i++) {
      assertTrue(emptyPartionsBitSet.get(i));
    }
  }

  @Test
  public void testConfigMerge() throws Exception {
    Configuration userPayloadConf = new Configuration(false);
    Configuration baseConf = new Configuration(false);

    userPayloadConf.set("local-key", "local-value");
    baseConf.set("base-key", "base-value");
    OutputContext outputContext = OutputTestHelpers.createOutputContext(
        userPayloadConf, baseConf, new Path("/"));
    UnorderedPartitionedKVOutput output =
        new UnorderedPartitionedKVOutput(outputContext, 1);
    output.initialize();
    Configuration mergedConf = output.conf;
    assertEquals("base-value", mergedConf.get("base-key"));
    assertEquals("local-value", mergedConf.get("local-key"));
  }
}
