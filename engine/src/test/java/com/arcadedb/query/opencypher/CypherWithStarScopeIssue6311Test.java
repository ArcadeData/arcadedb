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
 * Issue #6311: {@code WITH *} is an identity projection, so putting one between a MATCH and what reads it must
 * leave both the rows and the bindings alone. It did not: two comma-separated patterns of the same MATCH that
 * share a variable were joined on it without the projection and multiplied out into a Cartesian product with it.
 * <p>
 * The join is performed by the identity check each hop runs against the names already bound when it executes.
 * Those names used to be handed to the step as the planner's own live, mutable set, which a following WITH
 * clears - so the check silently stopped seeing the shared variable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherWithStarScopeIssue6311Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-withstar-6311").create();
    cypher("CREATE (a:l1 {tag:'a'})<-[:R0 {k7:false,k8:true}]-(n0 {tag:'n0a',k2:true})<-[:rt0]-(x)<-[:R2]-(n1 {tag:'A'})");
    cypher("CREATE (a:l1 {tag:'b'})<-[:R0 {k7:false,k8:true}]-(n0 {tag:'n0b',k2:true})<-[:rt0]-(x)<-[:R2]-(n1 {tag:'B'})");
    cypher("MATCH (n1 {tag:'A'}) CREATE (n2 {tag:'n2a'})<-[:rt8 {k3:'d',k10:-1957561185}]-(y)-[:R3]->(n1)");
    cypher("MATCH (n1 {tag:'B'}) CREATE (n2 {tag:'n2b'})<-[:rt8 {k3:'d',k10:-1957561185}]-(y)-[:R3]->(n1)");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void withStarKeepsOptionalMatchCardinality() {
    final String pattern = """
        OPTIONAL MATCH (:l1)<-[r0 {k7:false,k8:true}]-(n0 {k2:true})<-[r1:rt0]-()<-[r2]-(n1),
          (n2)<-[:rt8 {k3:'d',k10:-1957561185}]-()-[]->(n1)
        """;

    assertThat(count(pattern + "RETURN count(*) AS row_count")).isEqualTo(2);
    assertThat(count(pattern + "WITH *\nRETURN count(*) AS row_count")).isEqualTo(2);
  }

  @Test
  void withStarKeepsMatchCardinality() {
    // Same defect without OPTIONAL: the shared variable is what joins the two comma-separated patterns.
    final String pattern = "MATCH (n0 {k2:true})<-[:rt0]-()<-[]-(n1), (n2)<-[:rt8]-()-[]->(n1)\n";
    final String projection = "RETURN n0.tag AS n0, n1.tag AS n1, n2.tag AS n2";

    assertThat(rows(pattern + projection)).containsExactlyInAnyOrder("n0a|A|n2a", "n0b|B|n2b");
    assertThat(rows(pattern + "WITH *\n" + projection)).containsExactlyInAnyOrder("n0a|A|n2a", "n0b|B|n2b");
  }

  @Test
  void withStarKeepsTheJoinOnAVariableLengthPattern() {
    // A variable-length hop resolves its already-bound target from the incoming row rather than from a
    // plan-time variable set, so it never had the live-set exposure the fixed-length hop did. Pinned here
    // because it is the branch a reader would expect to share the defect.
    final String pattern = "MATCH (n0 {k2:true})<-[:rt0]-()<-[]-(n1), (n2)<-[:rt8*1..2]-()-[]->(n1)\n";
    final String projection = "RETURN n0.tag AS n0, n1.tag AS n1, n2.tag AS n2";

    assertThat(rows(pattern + projection)).containsExactlyInAnyOrder("n0a|A|n2a", "n0b|B|n2b");
    assertThat(rows(pattern + "WITH *\n" + projection)).containsExactlyInAnyOrder("n0a|A|n2a", "n0b|B|n2b");
  }

  @Test
  void explicitProjectionOfTheSharedVariableBehavesTheSame() {
    final String pattern = "MATCH (n0 {k2:true})<-[:rt0]-()<-[]-(n1), (n2)<-[:rt8]-()-[]->(n1)\n";
    assertThat(rows(pattern + "WITH n0, n1, n2\nRETURN n0.tag AS n0, n1.tag AS n1, n2.tag AS n2"))
        .containsExactlyInAnyOrder("n0a|A|n2a", "n0b|B|n2b");
  }

  @Test
  void withStarKeepsVariablesBoundForTheNextMatch() {
    // WITH * forwards the incoming scope, so the following MATCH must recognise x as already bound and
    // expand from it rather than re-scanning every vertex.
    assertThat(rows("""
        MATCH (n0 {k2:true})<-[:rt0]-(x)
        WITH *
        MATCH (x)<-[]-(n1)
        RETURN n0.tag AS n0, n1.tag AS n1, '' AS n2"""))
        .containsExactlyInAnyOrder("n0a|A|", "n0b|B|");
  }

  @Test
  void withStarPlusAliasKeepsBothTheScopeAndTheAlias() {
    assertThat(rows("""
        MATCH (n0 {k2:true})<-[:rt0]-(x)
        WITH *, n0.tag AS renamed
        MATCH (x)<-[]-(n1)
        RETURN n0.tag AS n0, n1.tag AS n1, renamed AS n2"""))
        .containsExactlyInAnyOrder("n0a|A|n0a", "n0b|B|n0b");
  }

  private long count(final String query) {
    final long[] value = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();
        value[0] = ((Number) rs.next().getProperty("row_count")).longValue();
      }
    });
    return value[0];
  }

  private List<String> rows(final String query) {
    final List<String> out = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          out.add(r.getProperty("n0") + "|" + r.getProperty("n1") + "|" + r.<Object>getProperty("n2"));
        }
      }
    });
    return out;
  }

  private void cypher(final String query) {
    database.transaction(() -> database.command("opencypher", query));
  }
}
