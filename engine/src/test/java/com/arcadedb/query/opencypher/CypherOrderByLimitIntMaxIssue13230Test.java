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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13230: {@code ORDER BY ... LIMIT 2147483647} (Integer.MAX_VALUE, the usual "no practical limit") failed with a bare
 * {@code IllegalArgumentException}: the top-K heap was sized with {@code Math.min(k + 1, 1000)}, and {@code k + 1} wraps to a
 * negative capacity for k = Integer.MAX_VALUE. {@code LIMIT 2147483646} and every smaller value worked.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherOrderByLimitIntMaxIssue13230Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      for (int i = 1; i <= 5; i++)
        database.command("opencypher", "CREATE (:T {k: $k, v: $v})", Map.of("k", i, "v", i % 3));
    });
  }

  private List<Long> column(final String query, final String column, final Object limit) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, Map.of("l", limit))) {
      rs.forEachRemaining(r -> values.add(r.<Number>getProperty(column).longValue()));
    }
    return values;
  }

  @Test
  void orderByLimitIntMaxAnswersEveryRow() {
    assertThat(column("MATCH (a:T) RETURN a.k AS k ORDER BY k LIMIT $l", "k", Integer.MAX_VALUE)).containsExactly(1L, 2L, 3L, 4L, 5L);
  }

  @Test
  void orderByDescLimitIntMaxKeepsTheOrder() {
    assertThat(column("MATCH (a:T) RETURN a.k AS k ORDER BY k DESC LIMIT $l", "k", Integer.MAX_VALUE)).containsExactly(5L, 4L, 3L, 2L, 1L);
  }

  @Test
  void distinctOrderByLimitIntMax() {
    assertThat(column("MATCH (a:T) RETURN DISTINCT a.v AS v ORDER BY v LIMIT $l", "v", Integer.MAX_VALUE)).containsExactly(0L, 1L, 2L);
  }

  @Test
  void withOrderByLimitIntMax() {
    assertThat(column("MATCH (a:T) WITH a ORDER BY a.k LIMIT $l RETURN a.k AS k", "k", Integer.MAX_VALUE)).containsExactly(1L, 2L, 3L, 4L, 5L);
  }

  @Test
  void aggregateOrderByLimitIntMax() {
    assertThat(column("MATCH (a:T) RETURN a.v AS v, count(*) AS c ORDER BY c DESC, v LIMIT $l", "v", Integer.MAX_VALUE))
        .containsExactly(1L, 2L, 0L);
  }

  @Test
  void literalLimitIntMax() {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:T) RETURN a.k AS k ORDER BY k LIMIT 2147483647")) {
      rs.forEachRemaining(r -> values.add(r.<Number>getProperty("k").longValue()));
    }
    assertThat(values).containsExactly(1L, 2L, 3L, 4L, 5L);
  }

  @Test
  void limitJustBelowIntMaxStillWorks() {
    assertThat(column("MATCH (a:T) RETURN a.k AS k ORDER BY k LIMIT $l", "k", Integer.MAX_VALUE - 1)).containsExactly(1L, 2L, 3L, 4L, 5L);
  }
}
