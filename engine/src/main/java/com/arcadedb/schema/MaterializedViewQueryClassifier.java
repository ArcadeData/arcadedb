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
package com.arcadedb.schema;

import com.arcadedb.database.Database;

/**
 * Determines if a defining query is "simple" (eligible for per-record incremental refresh).
 * Simple queries have: single type in FROM, no GROUP BY, no aggregate functions,
 * no subqueries, no TRAVERSE, no JOINs.
 */
public class MaterializedViewQueryClassifier {

  public static boolean isSimple(final String sql, final Database database) {
    final String upper = sql.toUpperCase().trim();

    if (upper.contains("GROUP BY"))
      return false;
    if (upper.contains("SUM(") || upper.contains("COUNT(") || upper.contains("AVG(") ||
        upper.contains("MIN(") || upper.contains("MAX("))
      return false;
    if (upper.contains("TRAVERSE"))
      return false;
    if (upper.contains(" JOIN "))
      return false;

    // Check for subqueries
    final int firstSelect = upper.indexOf("SELECT");
    if (firstSelect >= 0) {
      final int secondSelect = upper.indexOf("SELECT", firstSelect + 6);
      if (secondSelect >= 0)
        return false;
    }

    // Count FROM clauses
    int fromCount = 0;
    int searchFrom = 0;
    while ((searchFrom = upper.indexOf("FROM ", searchFrom)) >= 0) {
      fromCount++;
      searchFrom += 5;
    }
    if (fromCount > 1)
      return false;

    return true;
  }
}
