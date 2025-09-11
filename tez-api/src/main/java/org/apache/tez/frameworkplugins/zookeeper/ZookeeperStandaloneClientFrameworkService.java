package org.apache.tez.frameworkplugins.zookeeper;

import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.client.registry.zookeeper.ZkFrameworkClient;
import org.apache.tez.frameworkplugins.ClientFrameworkService;

public class ZookeeperStandaloneClientFrameworkService implements ClientFrameworkService {
  @Override public Optional<FrameworkClient> createOrGetFrameworkClient(Configuration conf) {
    return Optional.of(new ZkFrameworkClient());
  }
}
