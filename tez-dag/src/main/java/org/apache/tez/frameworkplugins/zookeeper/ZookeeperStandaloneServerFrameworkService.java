package org.apache.tez.frameworkplugins.zookeeper;

import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.registry.AMRegistry;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.client.registry.zookeeper.ZkAMRegistry;
import org.apache.tez.frameworkplugins.AmExtensions;
import org.apache.tez.frameworkplugins.ServerFrameworkService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperStandaloneServerFrameworkService implements ServerFrameworkService {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperStandaloneServerFrameworkService.class);
  private ZkAMRegistry amRegistry;

  @Override
  public synchronized Optional<AMRegistry> createOrGetAMRegistry(Configuration conf) {
    if (amRegistry == null) {
      try {
        final String externalID = System.getenv(TezConstants.TEZ_AM_EXTERNAL_ID);
        amRegistry = new ZkAMRegistry(externalID);
        amRegistry.init(conf);
        amRegistry.start();
        LOG.info("Created Zookeeper based AM Registry with externalID: {}", externalID);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return Optional.of(amRegistry);
  }

  @Override
  public Optional<AmExtensions> createOrGetDAGAppMasterExtensions() {
    return Optional.of(new ZkStandaloneAmExtensions(this));
  }
}
