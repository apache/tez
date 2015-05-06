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

package org.apache.tez.runtime.library.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Indicates that an IOOperation was interrupted
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IOInterruptedException extends IOException {

  public IOInterruptedException(String message) {
    super(message);
  }

  public IOInterruptedException(String message, Throwable cause) {
    super(message, cause);
  }

  public IOInterruptedException(Throwable cause) {
    super(cause);
  }
}
