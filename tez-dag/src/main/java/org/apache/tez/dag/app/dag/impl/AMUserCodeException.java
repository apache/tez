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

package org.apache.tez.dag.app.dag.impl;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.dag.api.TezException;


/**
 * Wrap the exception of user code in AM. Now we only have 3 kinds of user code in AM:
 * <li>VertexManager</li>
 * <li>EdgeManager</li> 
 * <li>InputInitializer</li>
 */
@Private
public class AMUserCodeException extends TezException {

  private static final long serialVersionUID = -3642816091492797520L;

  public static enum Source {
    VertexManager,
    EdgeManager,
    InputInitializer
  }
  
  private Source source;
  
  public AMUserCodeException(Source source, String message, Throwable cause) {
    super(message, cause);
    this.source = source;
  }

  public AMUserCodeException(Source source, Throwable cause) {
    super(cause);
    this.source = source;
  }
  
  public Source getSource() {
    return source;
  }
}
