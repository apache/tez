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

package org.apache.tez.examples;

import java.util.Map;

public class JoinValidateConfigured extends JoinValidate {

  private final Map<String, String> lhsProps;
  private final Map<String, String> rhsProps;
  private final Map<String, String> validateProps;
  private final String dagNameSuffix;

  public JoinValidateConfigured(Map<String, String> lhsProps, Map<String, String> rhsProps,
                                Map<String, String> validateProps, String dagNameSuffix) {
    this.lhsProps = lhsProps;
    this.rhsProps = rhsProps;
    this.validateProps = validateProps;
    this.dagNameSuffix = dagNameSuffix;
  }

  @Override
  protected Map<String, String> getLhsVertexProperties() {
    return this.lhsProps;
  }

  @Override
  protected Map<String, String> getRhsVertexProperties() {
    return this.rhsProps;
  }

  @Override
  protected Map<String, String> getValidateVertexProperties() {
    return this.validateProps;
  }

  @Override
  protected String getDagName() {
    return "JoinValidate_" + dagNameSuffix;
  }
}
