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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6999: {@code MatchStatement.toString()} rendered the positive patterns only, so anything
 * that round-trips a MATCH through its text (a materialized view, a continuous aggregate, the statement cache) silently
 * lost every {@code NOT} pattern and widened its result set.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6999MatchStatementNotPatternToStringTest extends TestHelper {

  /** The documented shape: a friend-of-a-friend who is not already a direct friend. */
  private static final String QUERY = "MATCH {as: a, type: Person}-Friend->{as: b}-Friend->{as: c}, NOT {as: a}-Friend->{as: c} "
      + "RETURN a.name AS name";

  @Test
  void toStringRendersTheNotPatterns() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final MatchStatement original = (MatchStatement) parser.parse(QUERY);
    assertThat(original.getNotMatchExpressions()).hasSize(1);

    final String rendered = original.toString();
    assertThat(rendered).as("the rendered text must carry the negative pattern").contains(", NOT ");

    final MatchStatement reparsed = (MatchStatement) parser.parse(rendered);
    assertThat(reparsed.getNotMatchExpressions()).as("the NOT pattern must survive a parse/render/parse round trip").hasSize(1);
    assertThat(reparsed).isEqualTo(original);
    assertThat(reparsed.toString()).isEqualTo(rendered);
  }

  /**
   * A negative pattern followed by a positive one: the AST builder indexed the sparse list of NOT tokens by pattern
   * position, so it negated the wrong pattern in this shape, and the text it rendered was a different statement.
   */
  @Test
  void aNotPatternFollowedByAPositiveOneIsAssignedToTheRightPattern() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final MatchStatement original = (MatchStatement) parser.parse(
        "MATCH {as: p, type: Person}, NOT {as: p}-Friend->{as: q}, {as: p}-Knows->{as: k}, NOT {as: p}-Blocks->{as: b} RETURN p");

    assertThat(original.getMatchExpressions()).hasSize(2);
    assertThat(original.getMatchExpressions().get(1).toString()).contains("Knows");
    assertThat(original.getNotMatchExpressions()).hasSize(2);
    assertThat(original.getNotMatchExpressions().get(0).toString()).contains("Friend");
    assertThat(original.getNotMatchExpressions().get(1).toString()).contains("Blocks");

    final String rendered = original.toString();
    final MatchStatement reparsed = (MatchStatement) parser.parse(rendered);
    assertThat(reparsed.getMatchExpressions()).hasSize(2);
    assertThat(reparsed.getNotMatchExpressions()).hasSize(2);
    assertThat(reparsed).isEqualTo(original);
    assertThat(reparsed.toString()).isEqualTo(rendered);
  }

  @Test
  void theReRenderedStatementReturnsTheSameRowsAsTheOriginal() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("Friend");

    database.transaction(() -> {
      // ALICE -> BOB -> CAROL, WITH NO DIRECT ALICE -> CAROL EDGE
      final MutableVertex alice = database.newVertex("Person").set("name", "Alice").save();
      final MutableVertex bob = database.newVertex("Person").set("name", "Bob").save();
      final MutableVertex carol = database.newVertex("Person").set("name", "Carol").save();
      alice.newEdge("Friend", bob).save();
      bob.newEdge("Friend", carol).save();
      // DAVE -> EVE -> FRANK, AND DAVE IS ALREADY A DIRECT FRIEND OF FRANK
      final MutableVertex dave = database.newVertex("Person").set("name", "Dave").save();
      final MutableVertex eve = database.newVertex("Person").set("name", "Eve").save();
      final MutableVertex frank = database.newVertex("Person").set("name", "Frank").save();
      dave.newEdge("Friend", eve).save();
      eve.newEdge("Friend", frank).save();
      dave.newEdge("Friend", frank).save();
    });

    assertThat(names(QUERY)).as("only the chain with no direct edge survives the NOT pattern").containsExactly("Alice");

    final String rendered = new SQLAntlrParser(null).parse(QUERY).toString();
    assertThat(names(rendered)).as("the re-rendered statement must keep the negative filter; without it Dave is back")
        .containsExactly("Alice");
  }

  private List<String> names(final String query) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }
}
