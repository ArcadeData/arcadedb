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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code coll} list functions must compare elements the way Cypher's {@code =} and {@code IN} do, so {@code 1}
 * and {@code 1.0} are the same element (issue #8561).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8561CollNumericEqualityTest extends TestHelper {

  private Object one(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().getProperty("r");
    }
  }

  @Test
  void indexOfMatchesAcrossNumericTypes() {
    assertThat(((Number) one("RETURN coll.indexOf([1.0, 2], 1) AS r")).longValue()).isEqualTo(0L);
    assertThat(((Number) one("RETURN coll.indexOf([1, 2.0], 2) AS r")).longValue()).isEqualTo(1L);
    assertThat(((Number) one("RETURN coll.indexOf([1, 2], 3) AS r")).longValue()).isEqualTo(-1L);
  }

  @Test
  void distinctCollapsesNumericTwins() {
    assertThat((List<?>) one("RETURN coll.distinct([1, 1.0, 2, 'a', 'a']) AS r")).hasSize(3);
    assertThat(((Number) one("RETURN size(coll.toSet([1, 1.0])) AS r")).longValue()).isEqualTo(1L);
  }

  @Test
  void nestedNumericsAreComparedLikeCypherEquality() {
    assertThat((List<?>) one("RETURN coll.distinct([[1], [1.0], {a: 1}, {a: 1.0}]) AS r")).hasSize(2);
    assertThat(((Number) one("RETURN coll.indexOf([[1.0, 2]], [1, 2]) AS r")).longValue()).isEqualTo(0L);
  }

  @Test
  void rangeIndexOfAcceptsAnIntegralFloat() {
    assertThat(((Number) one("RETURN coll.indexOf(range(1, 5), 2.0) AS r")).longValue()).isEqualTo(1L);
    assertThat(((Number) one("RETURN coll.indexOf(range(1, 5), 2.5) AS r")).longValue()).isEqualTo(-1L);
  }

  @Test
  void distinctAndGroupByFollowNestedCypherEquality() {
    try (final ResultSet rs = database.query("opencypher", "UNWIND [[1], [1.0], [2]] AS x RETURN count(DISTINCT x) AS r")) {
      assertThat(((Number) rs.next().getProperty("r")).longValue()).isEqualTo(2L);
    }
    try (final ResultSet rs = database.query("opencypher", "UNWIND [[1], [1.0], [2]] AS x RETURN x, count(*) AS c ORDER BY c DESC")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void unionCollapsesNumericTwins() {
    assertThat((List<?>) one("RETURN coll.union([1], [1.0, 2]) AS r")).hasSize(2);
  }
}
