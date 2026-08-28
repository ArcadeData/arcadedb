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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6831: {@code MERGE ... ON CREATE SET} / {@code ON MATCH SET} used to be served by a
 * second, hand-maintained copy of the SET clause that understood only the {@code variable.property} shape. The
 * dynamic-key form, the expression-target form, the property-value type check and the simultaneous-assignment rule
 * were all missing from that copy, so a MERGE action behaved differently from the identical stand-alone SET.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMergeSetClauseParityIssue6831Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcypher-6831").create();
    database.getSchema().createVertexType("P");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void onMatchSetWritesDynamicKey() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    database.transaction(() -> database.command("opencypher", "MERGE (n:P {id: 1}) ON MATCH SET n[$k] = 5",
        Map.of("k", "score")));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.score AS score");
    assertThat(rs.next().<Number>getProperty("score").intValue()).isEqualTo(5);
  }

  @Test
  void onCreateSetWritesDynamicKey() {
    database.transaction(() -> database.command("opencypher", "MERGE (n:P {id: 7}) ON CREATE SET n[$k] = 'new'",
        Map.of("k", "origin")));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P {id: 7}) RETURN n.origin AS origin");
    assertThat(rs.next().<String>getProperty("origin")).isEqualTo("new");
  }

  @Test
  void onMatchSetWritesThroughExpressionTarget() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    database.transaction(() -> database.command("opencypher",
        "MERGE (n:P {id: 1}) ON MATCH SET (CASE WHEN true THEN n END).x = 1"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.x AS x");
    assertThat(rs.next().<Number>getProperty("x").intValue()).isEqualTo(1);
  }

  @Test
  void onMatchSetWithFalseExpressionTargetIsANoOp() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    database.transaction(() -> database.command("opencypher",
        "MERGE (n:P {id: 1}) ON MATCH SET (CASE WHEN false THEN n END).x = 1"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.x AS x");
    assertThat(rs.next().<Object>getProperty("x")).isNull();
  }

  @Test
  void onCreateSetRejectsAMapPropertyValue() {
    // The identical stand-alone SET already raises this; the MERGE action must not be the lenient one.
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MERGE (n:P {id: 2}) ON CREATE SET n.p = {a: 1}")))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");
  }

  @Test
  void onMatchSetIsASimultaneousAssignment() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1, x: 'left', y: 'right'})"));

    database.transaction(() -> database.command("opencypher",
        "MERGE (n:P {id: 1}) ON MATCH SET n.x = n.y, n.y = n.x"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.x AS x, n.y AS y");
    final var row = rs.next();
    assertThat(row.<String>getProperty("x")).isEqualTo("right");
    assertThat(row.<String>getProperty("y")).isEqualTo("left");
  }

  @Test
  void onMatchSetStillCopiesFromAnotherEntity() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1}), (:P {id: 2, tag: 'src'})"));

    database.transaction(() -> database.command("opencypher",
        "MATCH (src:P {id: 2}) MERGE (n:P {id: 1}) ON MATCH SET n += src"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P {tag: 'src'}) RETURN count(n) AS c");
    assertThat(rs.next().<Number>getProperty("c").intValue()).isEqualTo(2);
  }

  @Test
  void onMatchSetStillAddsLabels() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1})"));

    database.transaction(() -> database.command("opencypher", "MERGE (n:P {id: 1}) ON MATCH SET n:Extra"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:Extra) RETURN n.id AS id");
    assertThat(rs.next().<Number>getProperty("id").intValue()).isEqualTo(1);
  }

  @Test
  void onMatchSetRemovesPropertyWithNullValue() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {id: 1, gone: 'x'})"));

    database.transaction(() -> database.command("opencypher", "MERGE (n:P {id: 1}) ON MATCH SET n.gone = null"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:P) RETURN n.gone AS gone");
    assertThat(rs.next().<Object>getProperty("gone")).isNull();
  }
}
