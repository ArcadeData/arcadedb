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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12275: a property lookup on an aggregate (max(m).a, head(collect(m)).a, min(p).x) dropped its column or answered
 * null. Expected rows are the ones Neo4j 5 returns.
 */
class CypherPropertyOfAggregateIssue12275Test extends TestHelper {

  private Map<String, Object> row(final String statement) {
    try (final ResultSet rs = database.query("opencypher", statement)) {
      final Result result = rs.next();
      assertThat(rs.hasNext()).isFalse();
      return result.toMap();
    }
  }

  @Test
  void aloneInReturn() {
    assertThat(row("UNWIND [{a:1},{a:2}] AS m RETURN max(m).a AS a")).isEqualTo(Map.of("a", 2L));
    assertThat(row("UNWIND [{a:1},{a:2}] AS m RETURN head(collect(m)).a AS a")).isEqualTo(Map.of("a", 1L));
    assertThat(row("UNWIND [{a:1},{a:2}] AS m RETURN collect(m)[0].a AS a")).isEqualTo(Map.of("a", 1L));
    assertThat(row("UNWIND [date('2020-01-01'), date('2021-01-01')] AS d RETURN max(d).year AS y")).isEqualTo(Map.of("y", 2021L));
    assertThat(row("UNWIND [point({x:1,y:1}), point({x:2,y:0})] AS p RETURN min(p).x AS a")).isEqualTo(Map.of("a", 1.0));
  }

  @Test
  void nextToAnotherAggregate() {
    assertThat(row("UNWIND [{a:1},{a:2}] AS m RETURN max(m).a AS a, count(*) AS c")).isEqualTo(Map.of("a", 2L, "c", 2L));
  }

  @Test
  void nextToGroupingKey() {
    assertThat(row("UNWIND [{a:1},{a:2}] AS m RETURN 1 AS k, min(m).a AS a")).isEqualTo(Map.of("k", 1L, "a", 1L));
  }
}
