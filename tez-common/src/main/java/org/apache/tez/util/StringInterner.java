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

package org.apache.tez.util;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

/**
 * A class to replace the {@code String.intern()}. The {@code String.intern()}
 * has some well-known performance limitations, and should generally be avoided.
 * Prefer Google's interner over the JDK's implementation.
 */
public final class StringInterner {

  private static final Interner<String> STRING_INTERNER =
      Interners.newWeakInterner();

  private StringInterner() {
  }

  public static String intern(final String str) {
    return (str == null) ? null : STRING_INTERNER.intern(str);
  }
}
