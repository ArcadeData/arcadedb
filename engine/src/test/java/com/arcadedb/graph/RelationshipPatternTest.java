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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.RelationshipPattern.Entry;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.arcadedb.graph.Vertex.DIRECTION.BOTH;
import static com.arcadedb.graph.Vertex.DIRECTION.IN;
import static com.arcadedb.graph.Vertex.DIRECTION.OUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * APOC's relationship pattern syntax, parsed once for every reader (issue #12103).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RelationshipPatternTest extends TestHelper {

  @Test
  void parsesAlternativesWithTheirMarkers() {
    final RelationshipPattern pattern = RelationshipPattern.parse("test", " KNOWS> | <LIKES |FOLLOWS||>");
    assertThat(pattern.entries()).containsExactly(new Entry("KNOWS", OUT), new Entry("LIKES", IN), new Entry("FOLLOWS", BOTH),
        new Entry(null, OUT));
    assertThat(pattern.typeNames()).containsExactly("KNOWS", "LIKES", "FOLLOWS");
    assertThat(pattern.isEmpty()).isFalse();
  }

  @Test
  void aListIsTheAlternativesItHolds() {
    assertThat(RelationshipPattern.parse("test", List.of("ROAD>", "<RAIL")).entries())
        .containsExactly(new Entry("ROAD", OUT), new Entry("RAIL", IN));
  }

  @Test
  void nullAndBlankAreEveryRelationshipBothWays() {
    for (final Object blank : new Object[] { null, "", "  ", " | ", List.of() }) {
      final RelationshipPattern pattern = RelationshipPattern.parse("test", blank);
      assertThat(pattern.isEmpty()).as("%s", blank).isTrue();
      assertThat(pattern.typesFor(OUT)).as("%s", blank).isEmpty();
      assertThat(pattern.typesFor(IN)).as("%s", blank).isEmpty();
      assertThat(pattern.commonDirection()).as("%s", blank).isEqualTo(BOTH);
    }
  }

  @Test
  void typesForAnswersTheTypesOneDirectionFollows() {
    final RelationshipPattern mixed = RelationshipPattern.parse("test", "KNOWS>|<LIKES|FOLLOWS");
    assertThat(mixed.typesFor(OUT)).containsExactly("KNOWS", "FOLLOWS");
    assertThat(mixed.typesFor(IN)).containsExactly("LIKES", "FOLLOWS");

    // null: nothing followed that way; empty: every type
    assertThat(RelationshipPattern.parse("test", "KNOWS>").typesFor(IN)).isNull();
    assertThat(RelationshipPattern.parse("test", ">").typesFor(OUT)).isEmpty();
    assertThat(RelationshipPattern.parse("test", ">").typesFor(IN)).isNull();
    assertThat(RelationshipPattern.parse("test", "KNOWS>|>").typesFor(OUT)).isEmpty();
    // a repeated alternative is one type to walk
    assertThat(RelationshipPattern.parse("test", "KNOWS|KNOWS>").typesFor(OUT)).containsExactly("KNOWS");
  }

  @Test
  void commonDirectionIsNullOnlyWhenTheAlternativesMixDirections() {
    assertThat(RelationshipPattern.parse("test", "KNOWS>|LIKES>").commonDirection()).isEqualTo(OUT);
    assertThat(RelationshipPattern.parse("test", "<KNOWS|<").commonDirection()).isEqualTo(IN);
    assertThat(RelationshipPattern.parse("test", "KNOWS|LIKES").commonDirection()).isEqualTo(BOTH);
    assertThat(RelationshipPattern.parse("test", "KNOWS>|<LIKES").commonDirection()).isNull();
    assertThat(RelationshipPattern.parse("test", "KNOWS>|LIKES").commonDirection()).isNull();
  }

  @Test
  void withUnmarkedGivesTheUnmarkedAlternativesTheDirection() {
    final RelationshipPattern mixed = RelationshipPattern.parse("test", "KNOWS>|LIKES|<FOLLOWS");
    assertThat(mixed.withUnmarked(IN).entries()).containsExactly(new Entry("KNOWS", OUT), new Entry("LIKES", IN),
        new Entry("FOLLOWS", IN));
    assertThat(mixed.withUnmarked(IN).typesFor(OUT)).containsExactly("KNOWS");
    // the parsed pattern is not changed
    assertThat(mixed.entries().get(1)).isEqualTo(new Entry("LIKES", BOTH));
    assertThat(mixed.withUnmarked(BOTH)).isSameAs(mixed);

    // every alternative carries a marker: nothing to change
    final RelationshipPattern marked = RelationshipPattern.parse("test", "KNOWS>|<LIKES");
    assertThat(marked.withUnmarked(OUT)).isSameAs(marked);
    // one shared direction once applied: the search can say it with the direction option
    assertThat(RelationshipPattern.parse("test", "KNOWS>|LIKES").withUnmarked(OUT).commonDirection()).isEqualTo(OUT);

    // the empty pattern is every relationship in that direction
    final RelationshipPattern every = RelationshipPattern.parse("test", "").withUnmarked(OUT);
    assertThat(every.entries()).containsExactly(new Entry(null, OUT));
    assertThat(every.typesFor(OUT)).isEmpty();
    assertThat(every.typesFor(IN)).isNull();
  }

  @Test
  void bothMarkersOnOneAlternativeAreRefused() {
    assertThatThrownBy(() -> RelationshipPattern.parse("node.degree", "LIKES|<KNOWS>"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("node.degree()")
        .hasMessageContaining("'<KNOWS>' carries both direction markers")
        .hasMessageContaining("write 'KNOWS'");
    assertThatThrownBy(() -> RelationshipPattern.parse("test", "<>"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("leave the filter empty");
  }

  @Test
  void aMapIsRefused() {
    assertThatThrownBy(() -> RelationshipPattern.parse("test", Map.of("KNOWS", ">")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("got a map");
  }

  @Test
  void followsReadsTheDirectionTheEdgeIsMetOnAndTheTypeHierarchy() {
    database.getSchema().createVertexType("P");
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("LIKES");
    database.getSchema().createEdgeType("BEST_FRIEND").addSuperType("KNOWS");

    final Edge[] edges = new Edge[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").save();
      final MutableVertex b = database.newVertex("P").save();
      edges[0] = a.newEdge("KNOWS", b).save();
      edges[1] = a.newEdge("BEST_FRIEND", b).save();
    });
    final Edge knows = edges[0];
    final Edge bestFriend = edges[1];

    final RelationshipPattern outgoing = RelationshipPattern.parse("test", "KNOWS>");
    assertThat(outgoing.follows(knows, OUT)).isTrue();
    assertThat(outgoing.follows(knows, IN)).isFalse();
    // a subtype is followed as its supertype is
    assertThat(outgoing.follows(bestFriend, OUT)).isTrue();

    assertThat(RelationshipPattern.parse("test", "<KNOWS").follows(knows, IN)).isTrue();
    assertThat(RelationshipPattern.parse("test", "<KNOWS").follows(knows, OUT)).isFalse();
    assertThat(RelationshipPattern.parse("test", "LIKES").follows(knows, OUT)).isFalse();
    assertThat(RelationshipPattern.parse("test", "LIKES|>").follows(knows, OUT)).isTrue();
    assertThat(RelationshipPattern.parse("test", "").follows(knows, IN)).isTrue();
  }
}
