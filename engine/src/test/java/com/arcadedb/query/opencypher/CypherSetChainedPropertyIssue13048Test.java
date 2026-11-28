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
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #13048: a SET target with more than one property accessor ({@code SET m.n.q = 7}) joined the
 * accessors into one property name and wrote it on the first variable. A map holding a node therefore received the
 * write instead of the node (silently dropped: no error, no change), and {@code SET n.a.b = 1} created a property named
 * {@code a.b}. The last accessor is the property written; the ones before it name the record, as in
 * {@code SET (m.n).q = 7}.
 */
class CypherSetChainedPropertyIssue13048Test extends TestHelper {

  private void run(final String query) {
    database.transaction(() -> database.command("opencypher", query).close());
  }

  private Result single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next();
    }
  }

  private Object nodeProperty(final long id, final String property) {
    return single("MATCH (n:T {id: " + id + "}) RETURN n.`" + property + "` AS p").getProperty("p");
  }

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:T {id: 1}), (:T {id: 2})").close();
  }

  @Test
  void aNodeHeldInAMapIsWritten() {
    run("MATCH (n:T {id: 1}) WITH {node: n} AS m SET m.node.q = 7");

    assertThat(nodeProperty(1, "q")).isEqualTo(7L);
    assertThat(nodeProperty(2, "q")).isNull();
  }

  @Test
  void aNodeNestedTwoMapsDeepIsWrittenAndReadBackThroughTheMap() {
    final Long[] read = new Long[2];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (n:T {id: 1}) WITH n, {a: {b: n}} AS m SET m.a.b.q = 5 RETURN m.a.b.q AS q, n.q AS nq")) {
        final Result row = rs.next();
        read[0] = row.<Number>getProperty("q").longValue();
        read[1] = row.<Number>getProperty("nq").longValue();
      }
    });
    assertThat(read).containsExactly(5L, 5L);
    assertThat(nodeProperty(1, "q")).isEqualTo(5L);
  }

  @Test
  void theSameStatementWritesTheNodeOfEachRow() {
    run("MATCH (n:T) WITH {node: n, v: n.id * 10} AS m SET m.node.q = m.v");

    assertThat(nodeProperty(1, "q")).isEqualTo(10L);
    assertThat(nodeProperty(2, "q")).isEqualTo(20L);
  }

  @Test
  void aStatementRunTwiceWithDifferentParametersWritesEachTarget() {
    for (final long id : new long[] { 1, 2 })
      database.transaction(() -> database.command("opencypher",
          "MATCH (n:T {id: $id}) WITH {node: n} AS m SET m.node.q = $v", Map.of("id", id, "v", id * 100)).close());

    assertThat(nodeProperty(1, "q")).isEqualTo(100L);
    assertThat(nodeProperty(2, "q")).isEqualTo(200L);
  }

  @Test
  void twoItemsOnTheSameChainAreBothWritten() {
    run("MATCH (n:T {id: 1}) WITH {node: n} AS m SET m.node.q = 1, m.node.r = 2");

    assertThat(nodeProperty(1, "q")).isEqualTo(1L);
    assertThat(nodeProperty(1, "r")).isEqualTo(2L);
  }

  @Test
  void aRelationshipHeldInAMapIsWritten() {
    run("MATCH (a:T {id: 1}), (b:T {id: 2}) CREATE (a)-[:R {w: 1}]->(b)");
    run("MATCH ()-[r:R]->() WITH {rel: r} AS m SET m.rel.w = 9");

    assertThat(single("MATCH ()-[r:R]->() RETURN r.w AS w").<Number>getProperty("w").longValue()).isEqualTo(9L);
  }

  @Test
  void aNullTargetIsASkippedWrite() {
    run("OPTIONAL MATCH (n:T {id: 99}) WITH {node: n} AS m SET m.node.q = 1");

    assertThat(nodeProperty(1, "q")).isNull();
    assertThat(nodeProperty(2, "q")).isNull();
  }

  @Test
  void aMapTargetIsRefusedAndNothingIsWritten() {
    assertThatThrownBy(() -> run("MATCH (n:T {id: 1}) WITH n, {inner: {a: 1}} AS m SET n.p = 1, m.inner.q = 2"))
        .isInstanceOf(TypeMismatchException.class).hasMessageContaining("Node or Relationship");
    assertThat(nodeProperty(1, "p")).as("the clause is refused before any item is written").isNull();
  }

  @Test
  void aChainThroughANodePropertyThatIsNotARecordIsRefusedInsteadOfCreatingADottedProperty() {
    run("MATCH (n:T {id: 1}) SET n.a = 5");

    assertThatThrownBy(() -> run("MATCH (n:T {id: 1}) SET n.a.b = 1")).isInstanceOf(TypeMismatchException.class);
    final List<Object> keys = single("MATCH (n:T {id: 1}) RETURN keys(n) AS k").getProperty("k");
    assertThat(keys).doesNotContain("a.b");
  }

  @Test
  void aChainThroughAnAbsentPropertyIsASkippedWrite() {
    run("MATCH (n:T {id: 1}) SET n.absent.b = 1");

    final List<Object> keys = single("MATCH (n:T {id: 1}) RETURN keys(n) AS k").getProperty("k");
    assertThat(keys).doesNotContain("absent.b");
  }

  @Test
  void aBacktickedNameWithADotIsStillOneProperty() {
    run("MATCH (n:T {id: 1}) SET n.`a.b` = 1");

    assertThat(nodeProperty(1, "a.b")).isEqualTo(1L);
  }

  @Test
  void theParenthesisedFormStillWrites() {
    run("MATCH (n:T {id: 1}) WITH {node: n} AS m SET (m.node).q = 3");

    assertThat(nodeProperty(1, "q")).isEqualTo(3L);
  }
}
