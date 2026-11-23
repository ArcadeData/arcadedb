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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #13228: a SET or REMOVE whose target is a plain variable bound to a map, a list, a scalar or a path was skipped as if the
 * write had happened, while the same value as an expression target is refused (#11693). Neo4j 5.26 raises "Type mismatch: expected
 * Node or Relationship but was Map" (a label write: "expected Node") for every statement below, before anything is written.
 */
class CypherSetRemoveNonEntityTargetIssue13228Test extends TestHelper {

  @BeforeEach
  void createData() {
    database.transaction(() -> database.command("opencypher", "CREATE (:L {id: 1, p: 'x'})-[:R {w: 1}]->(:L {id: 2, p: 'y'})"));
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = {
      "WITH {a: 1} AS m SET m.a = 2 RETURN m",
      "WITH 1 AS x SET x.p = 2 RETURN x",
      "WITH {a: 1} AS m SET m += {b: 2} RETURN m",
      "WITH {a: 1} AS m SET m = {b: 2} RETURN m",
      "WITH {a: 1} AS m SET m['a'] = 2 RETURN m",
      "WITH 'x' AS s SET s:Foo RETURN s",
      "MATCH (n:L) WITH n.id AS i SET i.p = 1 RETURN i",
      "MATCH (n:L) WITH collect(n) AS ns SET ns.p = 1 RETURN size(ns) AS s",
      "MATCH p = (a:L)-[:R]->(b:L) SET p.x = 1 RETURN p IS NOT NULL AS ok",
      "WITH [1] AS l REMOVE l.p RETURN l",
      "WITH {a: 1} AS m REMOVE m.a RETURN m",
      "WITH {a: 1} AS m REMOVE m['a'] RETURN m",
      "WITH 'x' AS s REMOVE s:Foo RETURN s",
      "MATCH (n:L) WITH collect(n) AS ns REMOVE ns.p RETURN size(ns) AS s",
      "MATCH p = (a:L)-[:R]->(b:L) REMOVE p.x RETURN p IS NOT NULL AS ok",
      "MATCH (a:L)-[r:R]->(b:L) SET r:Foo RETURN r",
      "MATCH (a:L)-[r:R]->(b:L) REMOVE r:Foo RETURN r",
      "UNWIND [{a: 1}] AS m SET m.seen = true",
      "MATCH (n:L {id: 1}) WITH n, {a: 1} AS m SET n.p = 'changed', m.a = 2",
      "MATCH (n:L {id: 1}) WITH n, {a: 1} AS m REMOVE n.p, m.a" })
  void aTargetThatIsNotAGraphElementIsRefusedAndNothingIsWritten(final String statement) {
    final Throwable thrown = catchThrowable(() -> database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement)) {
        while (rs.hasNext())
          rs.next();
      }
    }));

    assertThat(thrown).as(statement).isNotNull();
    Throwable cause = thrown;
    while (cause != null && !(cause instanceof TypeMismatchException))
      cause = cause.getCause();
    assertThat(cause).as(statement + ": " + thrown).isInstanceOf(TypeMismatchException.class);
    assertThat(cause.getMessage()).as(statement).contains("expected Node");
    // the clause is refused as a whole: the item before the bad target (n.p = 'changed', REMOVE n.p) wrote nothing
    assertThat(property("MATCH (n:L {id: 1}) RETURN n.p AS v")).isEqualTo("x");
    assertThat(property("MATCH (a:L {id: 1}) RETURN labels(a) AS v").toString()).isEqualTo("[L]");
  }

  @Test
  void aNullTargetFromAnOptionalMatchIsStillANoOp() {
    database.command("opencypher", "OPTIONAL MATCH (n:Nope) SET n.p = 1");
    database.command("opencypher", "OPTIONAL MATCH (n:Nope) SET n += {p: 1}");
    database.command("opencypher", "OPTIONAL MATCH (n:Nope) SET n = {p: 1}");
    database.command("opencypher", "OPTIONAL MATCH (n:Nope) SET n:Foo");
    database.command("opencypher", "OPTIONAL MATCH (n:Nope) REMOVE n.p");
    database.command("opencypher", "OPTIONAL MATCH (n:Nope) REMOVE n:Foo");

    assertThat(property("MATCH (n:L {id: 1}) RETURN n.p AS v")).isEqualTo("x");
  }

  @Test
  void nodesAndRelationshipsAreStillWritten() {
    database.command("opencypher", "MATCH (n:L {id: 1}) SET n.p = 'a', n += {q: 1}");
    database.command("opencypher", "MATCH ()-[r:R]->() SET r.w = 2, r += {z: 3}");
    database.command("opencypher", "MATCH (n:L {id: 1}) SET n:Extra");
    database.command("opencypher", "MATCH (n:L {id: 2}) REMOVE n.p");
    database.command("opencypher", "UNWIND [1, 2] AS i MATCH (n:L {id: i}) SET n.hit = i");
    database.command("opencypher", "MATCH (n:L) WITH collect(n) AS ns UNWIND ns AS m SET m.viaList = true");

    assertThat(property("MATCH (n:L {id: 1}) RETURN n.p AS v")).isEqualTo("a");
    assertThat(property("MATCH (n:L {id: 1}) RETURN n.q AS v")).isEqualTo(1L);
    assertThat(property("MATCH ()-[r:R]->() RETURN r.w AS v")).isEqualTo(2L);
    assertThat(property("MATCH ()-[r:R]->() RETURN r.z AS v")).isEqualTo(3L);
    assertThat(property("MATCH (n:L {id: 1}) RETURN n:Extra AS v")).isEqualTo(true);
    assertThat(property("MATCH (n:L {id: 2}) RETURN n.p AS v")).isNull();
    assertThat(property("MATCH (n:L {id: 2}) RETURN n.hit AS v")).isEqualTo(2L);
    assertThat(property("MATCH (n:L {id: 1}) RETURN n.viaList AS v")).isEqualTo(true);
  }

  private Object property(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().getProperty("v");
    }
  }
}
