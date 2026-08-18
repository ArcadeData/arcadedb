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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6338: a label disjunction {@code (y:A|B)} written on a node a relationship expands
 * <b>into</b> matched nothing, while the same disjunction on the pattern anchor matched. The anchor is served by
 * {@code MatchNodeStep}, which has always had a disjunction branch; the expanded endpoint was filtered by
 * {@code MatchRelationshipStep}, which ANDed the alternatives.
 * <p>
 * Covers the disjunction in each position of a chain - anchor, intermediate and final - because only the anchor was
 * exercised before, plus the two neighbouring shapes that must not change: a conjunction still requires every label,
 * and an alternative naming a type the schema does not have is simply an alternative that matches nothing rather
 * than a filter that rejects every row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLabelDisjunctionOnExpandedNodeIssue6338Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-label-disjunction-expanded-6338");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Post {k:'p1'})");
      database.command("opencypher", "CREATE (:Topic {k:'t1'})");
      database.command("opencypher", "CREATE (:Author {k:'a1'})");
      database.command("opencypher", "MATCH (p:Post {k:'p1'}), (t:Topic {k:'t1'}) CREATE (p)-[:TAGGED]->(t)");
      database.command("opencypher", "MATCH (t:Topic {k:'t1'}) CREATE (t)-[:OTHER]->(t)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void disjunctionOnTheAnchorMatchesEitherLabel() {
    assertThat(keys("MATCH (y:Author|Topic) RETURN y.k AS k ORDER BY k")).containsExactly("a1", "t1");
  }

  @Test
  void disjunctionOnTheFinalNodeOfAHopMatchesEitherLabel() {
    assertThat(keys("MATCH (p:Post)-[:TAGGED]->(y:Author|Topic) RETURN y.k AS k")).containsExactly("t1");
  }

  @Test
  void disjunctionOnAnIntermediateNodeOfAChainMatchesEitherLabel() {
    assertThat(keys("MATCH (p:Post)-[:TAGGED]->(y:Author|Topic)-[:OTHER]->(z:Topic) RETURN y.k AS k"))
        .containsExactly("t1");
  }

  @Test
  void singleLabelAndBareNodeStillBehaveTheSameOnThatPosition() {
    assertThat(keys("MATCH (p:Post)-[:TAGGED]->(y)-[:OTHER]->(z:Topic) RETURN y.k AS k")).containsExactly("t1");
    assertThat(keys("MATCH (p:Post)-[:TAGGED]->(y:Topic)-[:OTHER]->(z:Topic) RETURN y.k AS k")).containsExactly("t1");
  }

  @Test
  void anAlternativeTheSchemaDoesNotHaveDoesNotRejectTheOthers() {
    assertThat(keys("MATCH (p:Post)-[:TAGGED]->(y:NoSuchLabel|Topic) RETURN y.k AS k")).containsExactly("t1");
  }

  @Test
  void aConjunctionOnTheExpandedNodeStillRequiresEveryLabel() {
    assertThat(keys("MATCH (p:Post)-[:TAGGED]->(y:Author:Topic) RETURN y.k AS k")).isEmpty();
  }

  @Test
  void disjunctionOnAVariableLengthTargetMatchesEitherLabel() {
    assertThat(keys("MATCH (p:Post)-[:TAGGED*1..2]->(y:Author|Topic) RETURN y.k AS k")).containsExactly("t1");
  }

  @Test
  void disjunctionInAPatternComprehensionMatchesEitherLabel() {
    // A node pattern in expression position was built with the disjunction flag hardcoded to false, so (y:A|B)
    // reached the executor indistinguishable from (y:A:B) and the comprehension came back empty.
    assertThat(listKeys("MATCH (p:Post) RETURN [(p)-->(y:Author|Topic) | y.k] AS l")).containsExactly("t1");
    assertThat(listKeys("MATCH (p:Post) RETURN [(p)-->(y:Topic) | y.k] AS l")).containsExactly("t1");
    assertThat(listKeys("MATCH (p:Post) RETURN [(p)-->(y:Author:Topic) | y.k] AS l")).isEmpty();
  }

  @Test
  void disjunctionInAFunctionStyleExistsMatchesEitherLabel() {
    // The block form EXISTS { } takes the subquery path and always answered this correctly; the function form is
    // evaluated by PatternPredicateExpression, which ANDed the alternatives. Two spellings, one meaning.
    assertThat(keys("MATCH (a:Post) WHERE exists((a)-->(:Author|Topic)) RETURN a.k AS k")).containsExactly("p1");
    assertThat(keys("MATCH (a:Post) WHERE EXISTS { (a)-->(:Author|Topic) } RETURN a.k AS k")).containsExactly("p1");
    assertThat(keys("MATCH (a:Post) WHERE exists((a)-->(:Author:Topic)) RETURN a.k AS k")).isEmpty();
  }

  @Test
  void aLabelExpressionIsRefusedByTheClausesThatWrite() {
    // A disjunction says which labels a node MAY have, which only a read can answer. Neo4j refuses it in CREATE and
    // MERGE; ArcadeDB used to accept it and give it conjunction meaning, so MERGE missed the existing :Topic node
    // and created a second one under an invented Author~Topic type.
    assertThatThrownBy(() -> database.command("opencypher", "CREATE (n:Author|Topic {k:'new'})"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Label expressions are not allowed in CREATE");
    assertThatThrownBy(() -> database.command("opencypher", "MERGE (n:Author|Topic {k:'t1'})"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Label expressions are not allowed in MERGE");

    // Nothing was written by either attempt, and a conjunction is still accepted where it means something.
    assertThat(keys("MATCH (y {k:'t1'}) RETURN labels(y)[0] AS k")).containsExactly("Topic");
    database.command("opencypher", "CREATE (:Author:Topic {k:'both'})");
    assertThat(keys("MATCH (y:Author:Topic) RETURN y.k AS k")).containsExactly("both");
  }

  @Test
  void anAlternativeWhoseLabelIsOnlyKnownAtRuntimeIsRefusedToo() {
    // The refusal keys on the disjunction itself, not on how many labels came back with it: a dynamic $(expression)
    // alternative is collected separately from the static ones, so this shape carries a single static label and
    // would have slipped past a count-based guard - while being the one where a write acting on "A or B" is least
    // defensible, the second label not even being known until the query runs.
    assertThatThrownBy(() -> database.command("opencypher", "CREATE (n:Author|$('Topic') {k:'dyn'})"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Label expressions are not allowed in CREATE");
    assertThat(keys("MATCH (y {k:'dyn'}) RETURN y.k AS k")).isEmpty();
  }

  @SuppressWarnings("unchecked")
  private List<String> listKeys(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return (List<String>) rs.next().getProperty("l");
    }
  }

  private List<String> keys(final String query) {
    final List<String> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        keys.add(row.getProperty("k"));
      }
    }
    return keys;
  }
}
