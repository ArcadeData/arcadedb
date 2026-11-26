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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #13476: {@code UNWIND $rows AS item MERGE (i:L {id: item.id}) SET i:Extra} took O(n^2) because
 * {@code MergeStep} copied the statement's whole label-replacement map before every row. 20,000 rows took about 18 seconds; the
 * journal-based snapshot takes a fraction of a second, so the generous timeout only trips on a quadratic cost.
 */
class CypherMergeSetLabelScalesLinearlyIssue13476Test extends TestHelper {
  private static final int ROWS = 20_000;

  @Test
  @Timeout(value = 12, unit = TimeUnit.SECONDS)
  void mergeThenSetLabelOverManyRowsRelabelsEveryNodeOnce() {
    database.command("sql", "CREATE VERTEX TYPE Item13476");
    database.command("sql", "CREATE PROPERTY Item13476.id STRING");
    database.command("sql", "CREATE INDEX ON Item13476 (id) NOTUNIQUE");

    final List<Map<String, Object>> rows = new ArrayList<>(ROWS);
    for (int i = 0; i < ROWS; i++)
      rows.add(Map.of("id", "item-" + i, "v", (long) i));

    database.transaction(() -> database.command("opencypher",
        "UNWIND $rows AS item MERGE (i:Item13476 {id: item.id}) SET i.v = item.v, i:Container13476", Map.of("rows", rows)).close());

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Container13476:Item13476) RETURN count(n) AS c, sum(n.v) AS s")) {
      final var row = rs.next();
      assertThat(row.<Long>getProperty("c")).isEqualTo((long) ROWS);
      assertThat(row.<Long>getProperty("s")).isEqualTo((long) ROWS * (ROWS - 1) / 2);
    }
  }
}
