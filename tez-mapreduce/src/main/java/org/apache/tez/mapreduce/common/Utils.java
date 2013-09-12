package org.apache.tez.mapreduce.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.hadoop.mapred.MRCounters;

import com.google.common.base.Preconditions;

public class Utils {

  /**
   * Gets a handle to the Statistics instance based on the scheme associated
   * with path.
   *
   * @param path the path.
   * @param conf the configuration to extract the scheme from if not part of
   *   the path.
   * @return a Statistics instance, or null if none is found for the scheme.
   */
  @Private
  public static List<Statistics> getFsStatistics(Path path, Configuration conf) throws IOException {
    List<Statistics> matchedStats = new ArrayList<FileSystem.Statistics>();
    path = path.getFileSystem(conf).makeQualified(path);
    String scheme = path.toUri().getScheme();
    for (Statistics stats : FileSystem.getAllStatistics()) {
      if (stats.getScheme().equals(scheme)) {
        matchedStats.add(stats);
      }
    }
    return matchedStats;
  }

  public static Counter getMRCounter(TezCounter tezCounter) {
    Preconditions.checkNotNull(tezCounter);
    return new MRCounters.MRCounter(tezCounter);
  }
  
}
