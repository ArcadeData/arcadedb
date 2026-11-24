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

import java.math.BigInteger;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An integer past 2^53 and the floating point number next to it are different numbers: {@code =}, the inline map of a node and
 * the indexes already say so (issue #8888), but the inline map of a relationship, the pattern predicates, comprehensions and
 * EXISTS subqueries, variable-length patterns and MERGE compared through {@code double} first
 * ({@code ComparisonExpression.floatAwareEquals}) and found 2^53 + 1 equal to 2^53.0. The same pattern that MATCH answered with
 * nothing then matched in a MERGE, which left the float key uncreated.
 */
class CypherInlineMapIntegerBeyondDoublePrecisionTest extends TestHelper {
  /** 2^53 + 1: the first integer a double cannot hold. */
  private static final long BEYOND = 9007199254740993L;
  /** 2^53 as a double: what 2^53 + 1 rounds to. */
  private static final double ROUNDED = 9007199254740992.0d;

  @Test
  void aRelationshipPatternKeepsTheIntegerApartFromTheRoundedFloat() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:E {k: 1})-[:R {w: " + BEYOND + "}]->(:E {k: 2})");
    });
    final Map<String, Object> p = Map.of("f", ROUNDED);

    assertThat(count("MATCH (:E)-[r:R {w: $f}]->(:E) RETURN count(*) AS c", p)).as("inline map").isZero();
    assertThat(count("MATCH (:E)-[r:R]->(:E) WHERE r.w = $f RETURN count(*) AS c", p)).as("WHERE").isZero();
    assertThat(count("MATCH (a:E {k: 1}) WHERE (a)-[:R {w: $f}]->(:E) RETURN count(*) AS c", p)).as("pattern predicate").isZero();
    assertThat(count("MATCH (a:E {k: 1}) RETURN size([(a)-[:R {w: $f}]->(b) | b]) AS c", p)).as("pattern comprehension").isZero();
    assertThat(count("MATCH (a:E {k: 1})-[:R*1..2 {w: $f}]->(b) RETURN count(*) AS c", p)).as("variable-length").isZero();
    assertThat(count("MATCH (a:E {k: 1}) WHERE EXISTS { (a)-[:R {w: $f}]->() } RETURN count(*) AS c", p)).as("EXISTS").isZero();
  }

  @Test
  void mergeOfTheRoundedFloatDoesNotMatchTheInteger() {
    database.transaction(() -> database.command("opencypher", "CREATE (:E {n: " + BEYOND + "})"));
    final Map<String, Object> p = Map.of("f", ROUNDED);

    assertThat(count("MATCH (n:E {n: $f}) RETURN count(*) AS c", p)).as("MATCH of the same pattern").isZero();
    database.transaction(() -> database.command("opencypher", "MERGE (n:E {n: $f}) ON CREATE SET n.created = true", p));
    assertThat(count("MATCH (n:E) RETURN count(*) AS c", Map.of())).as("MERGE created the float key").isEqualTo(2L);
    assertThat(count("MATCH (n:E {created: true}) RETURN count(*) AS c", Map.of())).isEqualTo(1L);
  }

  @Test
  void mergeOfARelationshipKeyDoesNotMatchTheNeighbouringInteger() {
    database.transaction(() -> database.command("opencypher", "CREATE (:E {k: 1})-[:R {w: " + BEYOND + "}]->(:E {k: 2})"));
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:E {k: 1}), (b:E {k: 2}) MERGE (a)-[r:R {w: $f}]->(b)", Map.of("f", ROUNDED)));
    assertThat(count("MATCH ()-[r:R]->() RETURN count(r) AS c", Map.of())).isEqualTo(2L);
  }

  @Test
  void numbersThatAreEqualStillMatchAcrossIntegerAndFloat() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:E {k: 1})-[:R {w: 9007199254740992}]->(:E {k: 2}), (:E {k: 3})-[:R {w: 9007199254740994}]->(:E {k: 4}), (:E {n: 7})"));

    // 2^53 and 2^53 + 2 are exact doubles: the integer and the float are the same number
    assertThat(count("MATCH ()-[r:R {w: $f}]->() RETURN count(*) AS c", Map.of("f", 9007199254740992.0d))).isEqualTo(1L);
    assertThat(count("MATCH ()-[r:R {w: $f}]->() RETURN count(*) AS c", Map.of("f", 9007199254740994.0d))).isEqualTo(1L);
    assertThat(count("MATCH (a:E {k: 1}) WHERE (a)-[:R {w: $f}]->(:E) RETURN count(*) AS c", Map.of("f", 9007199254740992.0d))).isEqualTo(1L);
    // a small integer meets its float as before
    database.transaction(() -> database.command("opencypher", "MERGE (n:E {n: 7.0})"));
    assertThat(count("MATCH (n:E) RETURN count(*) AS c", Map.of())).isEqualTo(5L);
  }

  @Test
  void anIntegerBeyondTheLongRangeIsNotEqualToItsLowSixtyFourBits() {
    database.transaction(() -> database.command("opencypher", "CREATE (:E {k: 1, n: 1})-[:R {w: 1}]->(:E {k: 2})"));
    // 2^64 + 1: its low 64 bits are 1
    final Map<String, Object> p = Map.of("b", new BigInteger("18446744073709551617"));

    assertThat(count("MATCH (n:E {n: $b}) RETURN count(*) AS c", p)).as("node inline map").isZero();
    assertThat(count("MATCH ()-[r:R {w: $b}]->() RETURN count(*) AS c", p)).as("relationship inline map").isZero();
    assertThat(count("MATCH (a:E {k: 1}) WHERE (a)-[:R {w: $b}]->(:E) RETURN count(*) AS c", p)).as("pattern predicate").isZero();
    database.transaction(() -> database.command("opencypher", "MERGE (n:E {n: $b})", p));
    assertThat(count("MATCH (n:E) WHERE n.n IS NOT NULL RETURN count(*) AS c", Map.of())).as("MERGE created a node").isEqualTo(2L);

    // a BigInteger that holds a small number still is that number
    final Map<String, Object> one = Map.of("b", BigInteger.ONE);
    assertThat(count("MATCH ()-[r:R {w: $b}]->() RETURN count(*) AS c", one)).isEqualTo(1L);
  }

  private long count(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
