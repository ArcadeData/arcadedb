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
 * Issue #6977: a label write inside a {@code CALL { }} body has to remember what it displaced for the whole
 * statement, not for one plan.
 * <p>
 * A label change rewrites the vertex under a new type, so the original record is deleted and every other reference
 * to it - including the next row of the same clause - has to be redirected to the replacement. {@code SubqueryStep}
 * builds a <b>fresh execution plan for every outer row</b>, so the map that does the redirecting was born and died
 * inside a single row: the second outer row reached the body still holding the vertex the first one had deleted, and
 * the write followed its dangling edge list into
 * {@code VertexNotFoundException: Vertex #x:0 does not exist, so it cannot be deleted}.
 * <p>
 * Two outer rows are all it takes; the report needed {@code CALL db.schema()} only because that is what multiplied
 * one matched row into eight.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherSubqueryLabelWriteIssue6977Test {
  private Database database;

  @BeforeEach
  void setUp() {
    // Same guard OpenCypherConstraintTest uses: a run killed before @AfterEach leaves the directory behind, and
    // create() on an existing database throws, which would fail every method here for an environmental reason.
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/testopencypher-subquery-labelwrite-6977");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void removeLabelInsideASubqueryOutlivesTheRowItsPlanWasBuiltFor() {
    cypher("CREATE (b:Keep:Drop {name:'b'})-[:E {w:7}]->(a:Other:Drop {name:'a'})");

    // Three outer rows, all binding the same node: the first relabels it, the other two must recognise the
    // replacement instead of writing through the record it deleted.
    assertThat(writingRows("""
        UNWIND [1,2,3] AS i
        MATCH (n:Other {name:'a'})
        CALL (*) {
          REMOVE n:Drop
          RETURN n AS moved
        }
        RETURN moved.name + '/' + head(labels(moved)) + '/' + size(labels(moved)) AS v"""))
        .containsExactly("a/Other/1", "a/Other/1", "a/Other/1");

    assertThat(rows("MATCH (n {name:'a'}) RETURN labels(n) AS v")).containsExactly("[Other]");
    // The relationship is re-attached to the replacement, with its properties, exactly once.
    assertThat(rows("MATCH (x)-[r:E]->(y) RETURN x.name + '->' + y.name + '=' + r.w AS v")).containsExactly("b->a=7");
  }

  @Test
  void theOuterRowFollowsTheNodeTheSubqueryMoved() {
    cypher("CREATE (a:Other:Drop {name:'a'})");

    // The outer binding is the record the body deleted. Reading it after the CALL has to answer with the state the
    // write left behind, not with the vertex that is gone.
    assertThat(writingRows("""
        MATCH (n:Other {name:'a'})
        CALL (*) {
          REMOVE n:Drop
          RETURN 1 AS ignored
        }
        RETURN labels(n) AS v""")).containsExactly("[Other]");
  }

  @Test
  void removeLabelSurvivesTwoLevelsOfNestedSubqueries() {
    cypher("CREATE (a:Other:Drop {name:'a'})");

    // The map reaches the innermost body only if it is inherited at every level. Two levels is where a future
    // change to the isReadOnly()/inherit() chain would silently stop propagating it, and one level would not
    // notice.
    assertThat(writingRows("""
        UNWIND [1,2,3] AS i
        MATCH (n:Other {name:'a'})
        CALL (*) {
          CALL (*) {
            REMOVE n:Drop
            RETURN n AS innermost
          }
          RETURN innermost AS moved
        }
        RETURN moved.name + '/' + head(labels(moved)) + '/' + size(labels(moved)) AS v"""))
        .containsExactly("a/Other/1", "a/Other/1", "a/Other/1");

    assertThat(rows("MATCH (n {name:'a'}) RETURN labels(n) AS v")).containsExactly("[Other]");
  }

  @Test
  void setLabelInsideASubqueryOutlivesTheRowItsPlanWasBuiltFor() {
    // The mirror of the REMOVE case: SET n:Label runs the same replacement machinery.
    cypher("CREATE (a:Base {name:'a'})");

    assertThat(writingRows("""
        UNWIND [1,2,3] AS i
        MATCH (n:Base {name:'a'})
        CALL (*) {
          SET n:Extra
          RETURN n AS moved
        }
        RETURN labels(moved) AS v""")).containsExactly("[Base, Extra]", "[Base, Extra]", "[Base, Extra]");

    assertThat(rows("MATCH (n {name:'a'}) RETURN labels(n) AS v")).containsExactly("[Base, Extra]");
  }

  @Test
  void mergeOnMatchSetLabelInsideASubqueryOutlivesTheRowItsPlanWasBuiltFor() {
    cypher("CREATE (a:Base {name:'a'})");

    // MERGE re-reads the node from storage on every row, so the body itself always finds the live record. What
    // does not survive on its own is the alias the OUTER row imported: the first row's ON MATCH SET moved the
    // record out from under it.
    assertThat(writingRows("""
        MATCH (outer:Base {name:'a'})
        UNWIND [1,2,3] AS i
        CALL (*) {
          MERGE (n:Base {name:'a'})
          ON MATCH SET n:Extra
          RETURN n AS moved
        }
        RETURN labels(moved) + labels(outer) AS v"""))
        .containsExactly("[Base, Extra, Base, Extra]", "[Base, Extra, Base, Extra]", "[Base, Extra, Base, Extra]");

    assertThat(rows("MATCH (n {name:'a'}) RETURN labels(n) AS v")).containsExactly("[Base, Extra]");
  }

  @Test
  void labelIsRemovedOnceAcrossEveryOuterRowOfTheSubquery() {
    cypher("CREATE (a:Other:Drop {name:'a'})");

    final int[] labelsRemoved = new int[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", """
          UNWIND [1,2,3] AS i
          MATCH (n:Other {name:'a'})
          CALL (*) {
            REMOVE n:Drop
            RETURN 1 AS ignored
          }
          RETURN count(*) AS row_count""")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(((Number) rs.next().getProperty("row_count")).longValue()).isEqualTo(3);
        labelsRemoved[0] = rs.getStatistics().map(s -> s.getLabelsRemoved()).orElse(-1);
      }
    });

    assertThat(labelsRemoved[0]).isEqualTo(1);
  }

  @Test
  void theReportedSchemaCallAndSubqueryRemovalCompletes() {
    cypher("""
        CREATE (b:l0:l4:l6:l9:l10:l5 {k4: 'kN1omHAd', k8: 'b'})
          -[:rt5 {k3: true}]->
          (a:l11:l4 {k9: false, k8: 'a'})""");

    // CALL db.schema() yields one row per declared type, which fans the single matched row out into as many
    // invocations of the body - the shape the issue was reported with.
    final List<String> rows = writingRows("""
        CALL db.schema() YIELD nodes, relationships
        OPTIONAL MATCH (n5:l11 {k9: false}) <-[:rt5 {k3: true}]-
          (n6:l0&l4&l6&l9&l10&l5 {k4: "kN1omHAd"})
        WITH *
        WHERE n5 IS NOT NULL AND n6 IS NOT NULL
        CALL (*) {
          REMOVE n5:l4
          RETURN n5 AS n20, n6 AS n21
        }
        RETURN n20.k8 + '/' + head(labels(n20)) + '/' + size(labels(n20)) + '/' + n21.k8 AS v""");

    assertThat(rows).isNotEmpty();
    assertThat(rows).containsOnly("a/l11/1/b");

    assertThat(rows("MATCH (n {k8:'a'}) RETURN labels(n) AS v")).containsExactly("[l11]");
    assertThat(rows("MATCH (n {k8:'b'})-[r:rt5]->(m) RETURN m.k8 + '=' + r.k3 AS v")).containsExactly("a=true");
  }

  private List<String> rows(final String query) {
    return rows(query, false);
  }

  /**
   * Same as {@link #rows(String)} for a query that also writes, which the read-only entry point rejects.
   */
  private List<String> writingRows(final String query) {
    return rows(query, true);
  }

  private List<String> rows(final String query, final boolean writes) {
    final List<String> out = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = writes ? database.command("opencypher", query) : database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          out.add(String.valueOf(r.<Object>getProperty("v")));
        }
      }
    });
    return out;
  }

  private void cypher(final String query) {
    database.transaction(() -> database.command("opencypher", query));
  }
}
