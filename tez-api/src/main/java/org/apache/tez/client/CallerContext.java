/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.client;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

import com.google.common.base.Preconditions;

@Public
@Unstable
public class CallerContext {

  /**
   * Context in which Tez is being invoked.
   * For example, HIVE or PIG.
   */
  private String context;

  /**
   * Type of the caller. Should ideally be used along with callerId to uniquely identify the caller.
   * When used with YARN Timeline, this should map to the Timeline Entity Type.
   * For example, HIVE_QUERY_ID.
   */
  private String callerType;

  /**
   * Caller ID.
   * An ID to uniquely identify the caller within the callerType namespace
   */
  private String callerId;

  /**
   * Free-form text or a json-representation of relevant meta-data.
   * This can be used to describe the work being done. For example, for Hive,
   * this could be the Hive query text.
   */
  private String blob;

  /**
   * Private Constructor
   */
  private CallerContext() {
  }

  /**
   * Instantiate the Caller Context
   * @param context Context in which Tez is being invoked. For example, HIVE or PIG.
   * @param callerId Caller ID. An ID to uniquely identifier the caller within the callerType
   *                 namespace
   * @param callerType Type of the caller. Should ideally be used along with callerId to uniquely
   *                   identify the caller. When used with YARN Timeline, this should map to
   *                   the Timeline Entity Type. For example, HIVE_QUERY_ID.
   * @param blob Free-form text or a json-representation of relevant meta-data.
   *             This can be used to describe the work being done. For example, for Hive,
   *             this could be the Hive query text.
   * @return CallerContext
   */
  public static CallerContext create(String context, String callerId,
      String callerType, @Nullable String blob) {
    return new CallerContext(context, callerId, callerType, blob);
  }

  /**
   * Instantiate the Caller Context
   * @param context Context in which Tez is being invoked. For example, HIVE or PIG.
   * @param blob Free-form text or a json-representation of relevant meta-data.
   *             This can be used to describe the work being done. For example, for Hive,
   *             this could be the Hive query text.
   * @return CallerContext
   */
  @Private
  public static CallerContext create(String context, @Nullable String blob) {
    return new CallerContext(context, blob);
  }


  private CallerContext(String context, String callerId, String callerType,
      @Nullable String blob) {
    if (callerId != null || callerType != null) {
      setCallerIdAndType(callerId, callerType);
    }
    setContext(context);
    setBlob(blob);
  }

  private CallerContext(String context, @Nullable String blob) {
    setContext(context);
    setBlob(blob);
  }

  public String getCallerType() {
    return callerType;
  }

  public String getCallerId() {
    return callerId;
  }

  public String getBlob() {
    return blob;
  }

  public String getContext() {
    return context;
  }

  /**
   * @param context Context in which Tez is being invoked. For example, HIVE or PIG.
   */
  public CallerContext setContext(String context) {
    Preconditions.checkArgument(context != null && !context.isEmpty(),
        "Context cannot be null or empty");
    this.context = context;
    return this;
  }

  /**
   * @param callerId Caller ID. An ID to uniquely identifier the caller within the callerType
   *                 namespace
   * @param callerType Type of the caller. Should ideally be used along with callerId to uniquely
   *                   identify the caller. When used with YARN Timeline, this should map to
   *                   the Timeline Entity Type. For example, HIVE_QUERY_ID.
   */
  public CallerContext setCallerIdAndType(String callerId, String callerType) {
    Preconditions.checkArgument(callerType != null && !callerType.isEmpty()
        && callerId != null && !callerId.isEmpty(),
        "Caller Id and Caller Type cannot be null or empty");
    this.callerType = callerType;
    this.callerId = callerId;
    return this;
  }

  /**
   * @param blob Free-form text or a json-representation of relevant meta-data.
   *             This can be used to describe the work being done. For example, for Hive,
   *             this could be the Hive query text.
   */
  public CallerContext setBlob(@Nullable String blob) {
    this.blob = blob;
    return this;
  }

  @Override
  public String toString() {
    return "context=" + context
        + ", callerType=" + callerType
        + ", callerId=" + callerId
        + ", blob=" + blob;
  }

}
