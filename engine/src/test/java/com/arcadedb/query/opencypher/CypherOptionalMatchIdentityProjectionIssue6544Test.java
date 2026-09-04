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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6544: an OPTIONAL MATCH that carries a single-node named path
 * ({@code p = (n)}) alongside other pattern parts must bind the path variable exactly when the
 * whole pattern matches, whatever position the named path takes in the comma-separated list.
 * <p>
 * The reported symptom was a query whose row count changed when an identity {@code CASE}
 * projection was appended: the named path was dropped from the plan, so {@code WHERE p1 IS NOT
 * NULL} filtered the only row away.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherOptionalMatchIdentityProjectionIssue6544Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypher6544").create();
    database.transaction(() -> database.command("opencypher", "CREATE (a:V {id: 38})-[:R {k9: [1]}]->(b:V {id: 39})"));
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private List<Result> rows(final String query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  /**
   * The two queries of the issue: identical apart from a projection that returns every variable
   * unchanged, so they must agree on the number of rows.
   */
  @Test
  void identityProjectionDoesNotChangeCardinality() {
    database.transaction(() -> {
      final String control = """
          OPTIONAL MATCH () <-[r0:R]-(n0:V), p1 = (n0), (n0)
          WHERE all(item IN r0.k9 WHERE item IS NOT NULL)
          WITH *
          WHERE n0 IS NOT NULL AND p1 IS NOT NULL
          ORDER BY n0 ASC, p1 ASC
          RETURN null AS relationships
          """;
      final String identityProjection = """
          OPTIONAL MATCH () <-[r0:R]-(n0:V), p1 = (n0), (n0)
          WHERE all(item IN r0.k9 WHERE item IS NOT NULL)
          WITH *
          WHERE n0 IS NOT NULL AND p1 IS NOT NULL
          ORDER BY n0 ASC, p1 ASC
          WITH CASE WHEN n0 IS NULL THEN null ELSE n0 END AS n0,
               CASE WHEN r0 IS NULL THEN null ELSE r0 END AS r0,
               CASE WHEN p1 IS NULL THEN null ELSE p1 END AS p1
          RETURN null AS relationships
          """;

      assertThat(rows(control)).hasSize(1);
      assertThat(rows(identityProjection)).hasSize(1);
    });
  }

  /**
   * The same pattern returning its variables: the named path over the traversed node is bound.
   */
  @Test
  void namedPathAfterRelationshipPartIsBound() {
    database.transaction(() -> {
      final List<Result> rows = rows("""
          OPTIONAL MATCH () <-[r0:R]-(n0:V), p1 = (n0), (n0)
          RETURN n0.id AS id, r0.k9 AS k9, p1
          """);

      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).<Object>getProperty("id")).isEqualTo(38);
      assertThat(rows.get(0).<Object>getProperty("p1")).isNotNull();
    });
  }

  /**
   * The mirror image: the named path comes first, over a variable already bound by an earlier
   * clause. It used to be built into the plan and then dropped when the following pattern part
   * re-seated the head of the OPTIONAL chain.
   */
  @Test
  void namedPathBeforeRelationshipPartIsBound() {
    database.transaction(() -> {
      final List<Result> rows = rows("""
          MATCH (x:V)
          OPTIONAL MATCH p = (x), (x)-[r:R]->(z)
          RETURN x.id AS id, p, z.id AS zid
          ORDER BY id
          """);

      assertThat(rows).hasSize(2);
      // The whole optional pattern matches for the vertex with the outgoing edge: both variables bind.
      assertThat(rows.get(0).<Object>getProperty("id")).isEqualTo(38);
      assertThat(rows.get(0).<Object>getProperty("zid")).isEqualTo(39);
      assertThat(rows.get(0).<Object>getProperty("p")).isNotNull();
      // No outgoing edge: the pattern does not match, so the path is null as well.
      assertThat(rows.get(1).<Object>getProperty("id")).isEqualTo(39);
      assertThat(rows.get(1).<Object>getProperty("zid")).isNull();
      assertThat(rows.get(1).<Object>getProperty("p")).isNull();
    });
  }

  /**
   * A named path over a bound variable, on its own, still binds one path per input row. This shape
   * cannot regress the way the ones above can - with no other pattern part there is nothing to
   * re-seat the chain head, and the pattern cannot fail to match - so it pins the shape, not the
   * bug: it passes with and without the fix.
   */
  @Test
  void namedPathAloneOverBoundVariable() {
    database.transaction(() -> {
      final List<Result> rows = rows("""
          MATCH (x:V)
          OPTIONAL MATCH p = (x)
          RETURN x.id AS id, p
          ORDER BY id
          """);

      assertThat(rows).hasSize(2);
      assertThat(rows.get(0).<Object>getProperty("p")).isNotNull();
      assertThat(rows.get(1).<Object>getProperty("p")).isNotNull();
    });
  }

  /**
   * When the optional pattern does not match, every variable it introduces - the named path
   * included - is null, and the input row survives.
   */
  @Test
  void unmatchedOptionalNullsTheNamedPath() {
    database.transaction(() -> {
      final List<Result> driven = rows("""
          MATCH (x:V)
          OPTIONAL MATCH p = (x), (x)-[r:NONE]->(z)
          RETURN x.id AS id, p, z
          ORDER BY id
          """);
      assertThat(driven).hasSize(2);
      for (final Result row : driven) {
        assertThat(row.<Object>getProperty("p")).isNull();
        assertThat(row.<Object>getProperty("z")).isNull();
      }

      final List<Result> standalone = rows("""
          OPTIONAL MATCH () <-[r0:R]-(n0:V {id: 999}), p1 = (n0), (n0)
          RETURN n0, r0, p1
          """);
      assertThat(standalone).hasSize(1);
      assertThat(standalone.get(0).<Object>getProperty("n0")).isNull();
      assertThat(standalone.get(0).<Object>getProperty("r0")).isNull();
      assertThat(standalone.get(0).<Object>getProperty("p1")).isNull();
    });
  }
}
