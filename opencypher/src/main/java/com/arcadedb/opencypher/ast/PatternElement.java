/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.opencypher.ast;

/**
 * Base interface for pattern elements in Cypher queries.
 * Pattern elements include nodes and relationships.
 */
public interface PatternElement {
  /**
   * Returns the variable name for this pattern element.
   *
   * @return variable name or null if anonymous
   */
  String getVariable();

  /**
   * Returns true if this pattern element has a variable name.
   *
   * @return true if named, false if anonymous
   */
  default boolean hasVariable() {
    return getVariable() != null && !getVariable().isEmpty();
  }
}
