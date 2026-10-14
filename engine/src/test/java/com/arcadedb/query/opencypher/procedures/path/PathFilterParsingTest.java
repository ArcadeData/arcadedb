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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Parser-level checks of {@link RelationshipFilter} and {@link LabelFilter} (issue #10025): what each malformed input is
 * refused with, how the empty pieces of a filter read, and that a type filter follows the subtypes of the type it names.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PathFilterParsingTest {
  private static final String PROC = "path.test";

  private Database database;
  private Schema   schema;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-path-filter-parsing");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    schema = database.getSchema();
    schema.createVertexType("City");
    schema.createVertexType("Capital").addSuperType("City");
    schema.createEdgeType("ROAD");
    schema.createEdgeType("HIGHWAY").addSuperType("ROAD");
    schema.createEdgeType("RAIL");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void relationshipFilterDirectionsAndEmptyPieces() {
    final RelationshipFilter filter = RelationshipFilter.parse(PROC, "ROAD>|<RAIL", schema);
    assertThat(filter.typesFor(0, Vertex.DIRECTION.OUT)).containsExactly("ROAD");
    assertThat(filter.typesFor(0, Vertex.DIRECTION.IN)).containsExactly("RAIL");
    assertThat(filter.isSymmetric(0)).isFalse();

    assertThat(RelationshipFilter.parse(PROC, "ROAD", schema).isSymmetric(0)).isTrue();
    assertThat(RelationshipFilter.parse(PROC, "<", schema).typesFor(0, Vertex.DIRECTION.OUT)).isNull();
    assertThat(RelationshipFilter.parse(PROC, "<", schema).typesFor(0, Vertex.DIRECTION.IN)).isEmpty();

    // Blank, a lone pipe and null all mean "everything"
    for (final Object blank : new Object[] { null, "", "  ", "|", List.of() })
      assertThat(RelationshipFilter.parse(PROC, blank, schema)).isSameAs(RelationshipFilter.ANY);

    // An empty hop inside a sequence follows everything on that hop
    final RelationshipFilter sequence = RelationshipFilter.parse(PROC, "ROAD>,,<RAIL", schema);
    assertThat(sequence.typesFor(0, Vertex.DIRECTION.IN)).isNull();
    assertThat(sequence.typesFor(1, Vertex.DIRECTION.OUT)).isEmpty();
    assertThat(sequence.typesFor(1, Vertex.DIRECTION.IN)).isEmpty();
    assertThat(sequence.typesFor(2, Vertex.DIRECTION.IN)).containsExactly("RAIL");
    assertThat(sequence.typesFor(3, Vertex.DIRECTION.OUT)).containsExactly("ROAD");
  }

  @Test
  void relationshipFilterListFormIsOneHopOfAlternatives() {
    final RelationshipFilter filter = RelationshipFilter.parse(PROC, List.of("ROAD>", "<RAIL"), schema);
    assertThat(filter.typesFor(0, Vertex.DIRECTION.OUT)).containsExactly("ROAD");
    assertThat(filter.typesFor(0, Vertex.DIRECTION.IN)).containsExactly("RAIL");
    // A comma inside an element is not a sequence there, so it reaches the schema check as part of a name
    assertThatThrownBy(() -> RelationshipFilter.parse(PROC, List.of("ROAD>,<RAIL"), schema))
        .hasMessageContaining("does not exist");
  }

  @Test
  void relationshipFilterRefusals() {
    assertThatThrownBy(() -> RelationshipFilter.parse(PROC, "<ROAD>", schema))
        .hasMessage("path.test(): relationship filter '<ROAD>' carries both direction markers: write 'ROAD' to follow it in both directions");
    assertThatThrownBy(() -> RelationshipFilter.parse(PROC, "<>", schema))
        .hasMessageContaining("leave the filter empty");
    assertThatThrownBy(() -> RelationshipFilter.parse(PROC, Map.of("a", 1), schema))
        .hasMessage("path.test(): relationshipFilter must be a string or a list of strings, got a map");
    assertThatThrownBy(() -> RelationshipFilter.parse(PROC, "ROADS>", schema))
        .hasMessage("path.test(): relationship type 'ROADS' does not exist");
    assertThatThrownBy(() -> RelationshipFilter.parse(PROC, "City", schema))
        .hasMessage("path.test(): 'City' is not a relationship type, it is a vertex type");
  }

  @Test
  void labelFilterRefusals() {
    assertThatThrownBy(() -> parseLabels("-"))
        .hasMessage("path.test(): label filter entry '-' names no label");
    assertThatThrownBy(() -> parseLabels("City:"))
        .hasMessage("path.test(): label filter entry 'City:' has an empty label");
    assertThatThrownBy(() -> parseLabels("City:*"))
        .hasMessage("path.test(): label filter entry 'City:*' uses '*' inside a compound label: '*' stands alone");
    assertThatThrownBy(() -> parseLabels(Map.of("a", 1)))
        .hasMessage("path.test(): labelFilter must be a string or a list of strings, got a map");
    assertThatThrownBy(() -> parseLabels("City>"))
        .hasMessageContaining("ends with a direction marker: a label filter's operator ('+', '-', '/' or '>') goes before the label");
    assertThatThrownBy(() -> parseLabels("Town"))
        .hasMessage("path.test(): node label 'Town' does not exist");
    assertThatThrownBy(() -> parseLabels("ROAD"))
        .hasMessage("path.test(): 'ROAD' is not a node label, it is an edge type");
  }

  @Test
  void labelFilterBlankPiecesMeanNoFilter() {
    for (final Object blank : new Object[] { null, "", " , ", "|", List.of() })
      assertThat(parseLabels(blank).isUnfiltered()).isTrue();
    assertThat(parseLabels("City").isUnfiltered()).isFalse();
  }

  /**
   * A type filter follows the subtypes of the type it names, for relationships as for labels: an ArcadeDB type hierarchy
   * has no counterpart in Neo4j, and a query over the parent type means the whole hierarchy, as it does in MATCH.
   */
  @Test
  void typeFiltersFollowSubtypes() {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("City").set("name", "A").save();
      final MutableVertex b = database.newVertex("Capital").set("name", "B").save();
      final MutableVertex c = database.newVertex("City").set("name", "C").save();
      a.newEdge("HIGHWAY", b).save();
      b.newEdge("ROAD", c).save();
    });

    assertThat(names("CALL path.subgraphNodes(a, {relationshipFilter: 'ROAD>'}) YIELD node RETURN node")).containsExactly("A", "B", "C");
    assertThat(names("CALL path.subgraphNodes(a, {relationshipFilter: 'HIGHWAY>'}) YIELD node RETURN node")).containsExactly("A", "B");
    assertThat(names("CALL path.subgraphNodes(a, {labelFilter: '>City', minLevel: 1}) YIELD node RETURN node")).containsExactly("B", "C");
    assertThat(names("CALL path.subgraphNodes(a, {labelFilter: '/Capital'}) YIELD node RETURN node")).containsExactly("B");
  }

  private LabelFilter parseLabels(final Object filter) {
    return LabelFilter.parse(PROC, filter, schema, false, true, 0);
  }

  private List<String> names(final String call) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:City {name: 'A'}) " + call)) {
      while (rs.hasNext())
        names.add(((Vertex) rs.next().getProperty("node")).getString("name"));
    }
    return names;
  }
}
