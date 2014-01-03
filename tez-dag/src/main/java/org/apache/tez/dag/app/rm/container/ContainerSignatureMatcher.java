package org.apache.tez.dag.app.rm.container;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.LocalResource;

public interface ContainerSignatureMatcher {
  /**
   * Checks the compatibility between the specified container signatures.
   *
   * @return true if the first signature is a super set of the second
   *         signature.
   */
  public boolean isSuperSet(Object cs1, Object cs2);
  
  /**
   * Checks if the container signatures match exactly
   * @return true if exact match
   */
  public boolean isExactMatch(Object cs1, Object cs2);
  
  /**
   * Gets additional resources specified in lr2, which are not present for lr1
   * 
   * @param lr11
   * @param lr22
   * @return
   */
  public Map<String, LocalResource> getAdditionalResources(Map<String, LocalResource> lr1,
      Map<String, LocalResource> lr2);
}