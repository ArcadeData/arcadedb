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
package com.arcadedb.query.opencypher.executor.steps;

import java.util.Map;
import java.util.Set;

/**
 * Shared counting rules for Cypher mutation statistics, single-sourced so the subtle semantics do
 * not drift between the SET and MERGE step implementations.
 */
public final class CypherStatisticsHelper {
  private CypherStatisticsHelper() {
  }

  /**
   * Counts pre-existing, non-internal properties that a replace-map (SET n = {..} / MERGE ... = {..})
   * does not re-set with a non-null value. Neo4j counts these removals toward properties-set.
   */
  public static int countRemovedProperties(final Set<String> existingProps, final Map<String, Object> replacementMap) {
    int removed = 0;
    for (final String prop : existingProps)
      if (!prop.startsWith("@") && replacementMap.get(prop) == null)
        removed++;
    return removed;
  }
}
