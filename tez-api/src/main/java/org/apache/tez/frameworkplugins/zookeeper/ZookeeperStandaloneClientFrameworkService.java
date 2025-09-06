package org.apache.tez.frameworkplugins.zookeeper;

import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.client.registry.AMRegistryClient;
import org.apache.tez.client.registry.zookeeper.ZkAMRegistryClient;
import org.apache.tez.client.registry.zookeeper.ZkFrameworkClient;
import org.apache.tez.frameworkplugins.ClientFrameworkService;

public class ZookeeperStandaloneClientFrameworkService implements ClientFrameworkService {
  @Override public Optional<FrameworkClient> createOrGetFrameworkClient(Configuration conf) {
    return Optional.of(new ZkFrameworkClient());
  }

  @Override public Optional<AMRegistryClient> createOrGetRegistryClient(Configuration conf) {
    ZkAMRegistryClient registry = ZkAMRegistryClient.getClient(conf);
    try {
      registry.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return Optional.of(registry);
  }
}
