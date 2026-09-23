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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #8260: {@code ProjectionItem.getProjectionAlias()} rebuilt and escaped a fresh {@link
 * com.arcadedb.query.sql.parser.Identifier} on every call for a projection with no explicit {@code AS}, which on a
 * full-scan {@code GROUP BY} runs once per record for nothing. The default alias is now cached on the {@code
 * ProjectionItem} node after the first computation.
 * <p>
 * The parsed {@code ProjectionItem} nodes are reused across repeated executions of the same statement (the SQL
 * statement cache), so this pins that the cache does not go stale, and does not leak between distinct queries that
 * happen to share the query engine's cache, or between the pre-aggregate/aggregate/final split each GROUP BY copies
 * a projection item into.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8260DefaultAliasCacheTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE Sale");
      database.command("SQL", "INSERT INTO Sale SET mode = 'AIR', amount = 10");
      database.command("SQL", "INSERT INTO Sale SET mode = 'AIR', amount = 20");
      database.command("SQL", "INSERT INTO Sale SET mode = 'SHIP', amount = 5");
      database.command("SQL", "INSERT INTO Sale SET mode = 'RAIL', amount = 1");
      database.command("SQL", "INSERT INTO Sale SET mode = 'RAIL', amount = 2");
      database.command("SQL", "INSERT INTO Sale SET mode = 'RAIL', amount = 3");
    });
  }

  @Test
  void groupByWithoutExplicitAliasUsesThePropertyNameOnEveryRow() {
    for (int run = 0; run < 3; run++) {
      // RUN THE SAME STATEMENT TEXT REPEATEDLY: the parsed ProjectionItem nodes come from the SQL statement cache,
      // so a stale or cross-execution-leaked cached alias would surface on a later run, not necessarily the first.
      final Map<String, Long> counts = groupCounts("SELECT mode, count(*) AS n FROM Sale GROUP BY mode", "mode", "n");
      assertThat(counts).containsExactlyInAnyOrderEntriesOf(Map.of("AIR", 2L, "SHIP", 1L, "RAIL", 3L));
    }
  }

  @Test
  void groupByOnAComputedExpressionWithNoAliasDefaultsToItsOwnText() {
    // `mode` alone is a base identifier and takes the other branch of Expression.getDefaultAlias(); `mode || ''`
    // is not, so this exercises the this.toString() branch of the same cached call.
    final ResultSet rs = database.query("SQL", "SELECT mode || '' FROM Sale GROUP BY mode || '' ORDER BY mode || ''");

    final List<String> defaultAliasColumns = new ArrayList<>();
    while (rs.hasNext())
      defaultAliasColumns.addAll(rs.next().getPropertyNames());

    // every row exposes the same default-alias column name - a stale/misrendered cache would show up as a
    // different (or missing) column name on a later row
    assertThat(defaultAliasColumns).isNotEmpty();
    assertThat(defaultAliasColumns).allSatisfy(name -> assertThat(name).isEqualTo("mode || ''"));
  }

  @Test
  void distinctQueriesWithTheSameUnaliasedExpressionDoNotShareAStaleAlias() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE OtherSale");
      database.command("SQL", "INSERT INTO OtherSale SET mode = 'TRUCK', amount = 99");
    });

    final Map<String, Long> first = groupCounts("SELECT mode, count(*) AS n FROM Sale GROUP BY mode", "mode", "n");
    final Map<String, Long> second = groupCounts("SELECT mode, count(*) AS n FROM OtherSale GROUP BY mode", "mode", "n");

    assertThat(first).containsExactlyInAnyOrderEntriesOf(Map.of("AIR", 2L, "SHIP", 1L, "RAIL", 3L));
    assertThat(second).containsExactlyInAnyOrderEntriesOf(Map.of("TRUCK", 1L));
  }

  @Test
  void unaliasedAggregateProjectionSharesTheCachedIdentifierWithItsSplitCopySafely() {
    // count(*) WITHOUT "AS": splitForAggregation() copies this ProjectionItem into a pre-aggregate/aggregate/final
    // split, and the final item's explicit alias is set from the ORIGINAL item's (now cached) default alias
    // (ProjectionItem.splitForAggregation: `result.alias = getProjectionAlias();`), so the two items end up sharing
    // the same Identifier instance. Run it more than once to also exercise that shared instance across repeated
    // executions of the same cached statement.
    for (int run = 0; run < 3; run++) {
      final Map<String, Long> counts = groupCounts("SELECT mode, count(*) FROM Sale GROUP BY mode", "mode", "count(*)");
      assertThat(counts).containsExactlyInAnyOrderEntriesOf(Map.of("AIR", 2L, "SHIP", 1L, "RAIL", 3L));
    }
  }

  private Map<String, Long> groupCounts(final String query, final String keyProperty, final String countProperty) {
    final Map<String, Long> result = new LinkedHashMap<>();
    try (ResultSet rs = database.query("SQL", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        result.put(r.getProperty(keyProperty), ((Number) r.getProperty(countProperty)).longValue());
      }
    }
    return result;
  }
}
