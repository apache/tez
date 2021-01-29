/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.classification.InterfaceAudience;

@SuppressWarnings("unchecked")
public class NamedEntityDescriptor<T extends NamedEntityDescriptor<T>> extends EntityDescriptor<NamedEntityDescriptor<T>>  {
  private final String entityName;

  @InterfaceAudience.Private
  public NamedEntityDescriptor(String entityName, String className) {
    super(className);
    this.entityName = Objects.requireNonNull(entityName, "EntityName must be specified");
  }

  public String getEntityName() {
    return entityName;
  }

  public T setUserPayload(UserPayload userPayload) {
    super.setUserPayload(userPayload);
    return (T) this;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException(
        "write is not expected to be used for a NamedEntityDescriptor");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException(
        "readFields is not expected to be used for a NamedEntityDescriptor");
  }

  @Override
  public String toString() {
    boolean hasPayload =
        getUserPayload() == null ? false : getUserPayload().getPayload() == null ? false : true;
    return "EntityName=" + entityName + ", ClassName=" + getClassName() + ", hasPayload=" + hasPayload;
  }
}
