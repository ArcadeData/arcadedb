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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7629: {@code SET n += $p} raised {@code TypeError: InvalidPropertyType} when a
 * parameter map contained a nested map value, while {@code CREATE (n {m: $m})} silently stored the very same value.
 * <p>
 * Real Neo4j refuses a map-valued property everywhere - "Property values can only be of primitive types or arrays
 * thereof" applies to the stored value regardless of which clause writes it, or whether the map came from a literal
 * or a bound parameter (confirmed by the pre-existing {@code CypherMergeSetClauseParityIssue6831Test} and
 * {@code CypherSetFromEntityIssue6832Test}, which already reject the equivalent literal-map forms on SET/MERGE's
 * {@code ON MATCH SET}). So {@code SET}'s rejection in the reported repro is correct, Neo4j-faithful behaviour, not a
 * regression: the actual bug is that {@code CreateStep} and {@code MergeStep}'s creation branch never applied the
 * same check, silently storing a value no other openCypher write clause accepts. These tests pin CREATE and MERGE's
 * creation branch to the same behaviour as SET/MERGE's SET actions, for both a literal map and a parameter-sourced
 * one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMapPropertyConsistencyIssue7629Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcypher-7629").create();
    database.getSchema().createVertexType("R");
    database.getSchema().createEdgeType("REL");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
  }

  @Test
  void createRejectsAMapValuedParameterProperty() {
    final Map<String, Object> mapValue = Map.of("k", 1, "nested", Map.of("deep", true));

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "CREATE (n:R {id: 1, m: $m})", Map.of("m", mapValue))))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");

    assertThat(database.query("opencypher", "MATCH (n:R) RETURN n").hasNext()).isFalse();
  }

  @Test
  void createRejectsALiteralNestedMapProperty() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "CREATE (n:R {id: 1, m: {nested: 1}})")))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");
  }

  @Test
  void createRejectsAMapValuedEdgeProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (:R {id: 1}), (:R {id: 2})"));

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (a:R {id: 1}), (b:R {id: 2}) CREATE (a)-[r:REL {m: $m}]->(b)", Map.of("m", Map.of("k", 1)))))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");
  }

  /**
   * The bare-parameter property syntax ({@code CREATE (n:R $props)}) goes through
   * {@code CreateStep.setPropertiesFromParameter}, a different method from the {@code CREATE (n:R {m: $m})} map-
   * literal form's {@code setProperties} - this pins that it funnels through the same validation.
   */
  @Test
  void createRejectsAMapValuedPropertyViaBareParameterSyntax() {
    final Map<String, Object> props = Map.of("id", 1, "m", Map.of("k", 1));

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "CREATE (n:R $props)", Map.of("props", props))))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");

    assertThat(database.query("opencypher", "MATCH (n:R) RETURN n").hasNext()).isFalse();
  }

  /** The edge equivalent of {@link #createRejectsAMapValuedPropertyViaBareParameterSyntax}, going through
   *  {@code CreateStep.buildPropertiesFromParameter}. */
  @Test
  void createRejectsAMapValuedEdgePropertyViaBareParameterSyntax() {
    database.transaction(() -> database.command("opencypher", "CREATE (:R {id: 1}), (:R {id: 2})"));
    final Map<String, Object> props = Map.of("m", Map.of("k", 1));

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (a:R {id: 1}), (b:R {id: 2}) CREATE (a)-[r:REL $props]->(b)", Map.of("props", props))))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");
  }

  @Test
  void createStillAcceptsOrdinaryScalarAndListParameters() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (n:R {id: 1, s: $s, tags: $tags})", Map.of("s", "ok", "tags", List.of("x", "y"))));

    final ResultSet rs = database.query("opencypher", "MATCH (n:R) RETURN n.s AS s, n.tags AS tags");
    final var row = rs.next();
    assertThat(row.<String>getProperty("s")).isEqualTo("ok");
    assertThat(row.<List<Object>>getProperty("tags")).containsExactly("x", "y");
  }

  @Test
  void createRejectsAListContainingAMapParameter() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "CREATE (n:R {id: 1, tags: $tags})", Map.of("tags", List.of(Map.of("k", 1))))))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");

    assertThat(database.query("opencypher", "MATCH (n:R) RETURN n").hasNext()).isFalse();
  }

  /**
   * {@code point()} is internally a plain map of coordinate keys (ArcadeDB has no dedicated Geometry runtime type
   * yet, issue #4870), so a naive "reject every map property" check - the fix above - would also reject a Point,
   * which real Neo4j treats as a primitive property type. {@code CypherValues.isPointShaped} recognises one
   * structurally (the {@code x}/{@code y}/{@code crs} keys every branch of {@code CypherPointFunction} writes) so
   * the validator can tell the two apart; this pins that a Point still stores through CREATE (already covered more
   * broadly by {@code OpenCypherSpatialFunctionsTest} and {@code OpenCypherSpatialFunctionsComprehensiveTest}, which
   * regressed without it).
   */
  @Test
  void createStillAcceptsAPointValuedProperty() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (n:R {id: 1, loc: point({longitude: 12.5, latitude: 55.6})})"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:R) RETURN n.loc AS loc");
    final Object loc = rs.next().getProperty("loc");
    assertThat(loc).isInstanceOf(Map.class);
    assertThat(((Map<?, ?>) loc).get("latitude")).isEqualTo(55.6);
  }

  /**
   * A Point read back off storage is deserialized as a plain {@link Map}, with no trace of having come from
   * {@code point()} - which is why the exemption above has to be structural rather than by class identity. This
   * pins the shape that would break under an identity-based check: copying a previously-stored Point property into
   * a new node, in an entirely separate query/transaction from the one that created it.
   */
  @Test
  void createAcceptsAPointPropertyCopiedFromAPreviouslyStoredNode() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (n:R {id: 1, loc: point({longitude: 12.5, latitude: 55.6})})"));

    database.transaction(() -> database.command("opencypher",
        "MATCH (a:R {id: 1}) CREATE (b:R {id: 2, loc: a.loc})"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:R {id: 2}) RETURN n.loc AS loc");
    final Object loc = rs.next().getProperty("loc");
    assertThat(loc).isInstanceOf(Map.class);
    assertThat(((Map<?, ?>) loc).get("latitude")).isEqualTo(55.6);
  }

  /**
   * The Point exemption waives the "is a Map" check on a point-shaped map itself, but not on its own entries: a map
   * that pads out the x/y/crs keys a Point needs with an extra map-valued key must still be refused, or the
   * exemption would double as a general escape hatch from the whole check.
   */
  @Test
  void createRejectsAPointShapedMapSmugglingANestedMap() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "CREATE (n:R {id: 1, loc: {x: 1, y: 2, crs: 'cartesian', payload: {secret: 1}}})")))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");
  }

  @Test
  void createAcceptsAListOfPoints() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (n:R {id: 1, stops: [point({x: 1, y: 2}), point({x: 3, y: 4})]})"));

    final ResultSet rs = database.query("opencypher", "MATCH (n:R) RETURN n.stops AS stops");
    final List<Object> stops = rs.next().getProperty("stops");
    assertThat(stops).hasSize(2);
    assertThat(((Map<?, ?>) stops.get(0)).get("x")).isEqualTo(1.0);
    assertThat(((Map<?, ?>) stops.get(1)).get("x")).isEqualTo(3.0);
  }

  @Test
  void mergeCreationBranchRejectsAMapValuedParameterProperty() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MERGE (n:R {id: 1, m: $m})", Map.of("m", Map.of("k", 1)))))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");

    assertThat(database.query("opencypher", "MATCH (n:R) RETURN n").hasNext()).isFalse();
  }

  @Test
  void mergeCreationBranchRejectsAMapValuedEdgeProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (:R {id: 1}), (:R {id: 2})"));

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (a:R {id: 1}), (b:R {id: 2}) MERGE (a)-[r:REL {m: $m}]->(b)", Map.of("m", Map.of("k", 1)))))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");
  }

  /**
   * A null-valued property on the MERGE creation branch is simply not stored, matching CREATE - it isn't a removal
   * the way it is on SET's merge form ({@code ON MATCH SET n += {..}}), since there's nothing yet to remove it from.
   */
  @Test
  void mergeCreationBranchSkipsANullVertexProperty() {
    database.transaction(() -> database.command("opencypher",
        "MERGE (n:R {id: 1, s: $s})", Collections.singletonMap("s", null)));

    final ResultSet rs = database.query("opencypher", "MATCH (n:R {id: 1}) RETURN n.s AS s, keys(n) AS k");
    final var row = rs.next();
    assertThat(row.<Object>getProperty("s")).isNull();
    assertThat(row.<List<String>>getProperty("k")).doesNotContain("s");
  }

  /** The edge equivalent of {@link #mergeCreationBranchSkipsANullVertexProperty}. */
  @Test
  void mergeCreationBranchSkipsANullEdgeProperty() {
    database.transaction(() -> database.command("opencypher", "CREATE (:R {id: 1}), (:R {id: 2})"));

    database.transaction(() -> database.command("opencypher",
        "MATCH (a:R {id: 1}), (b:R {id: 2}) MERGE (a)-[r:REL {s: $s}]->(b)", Collections.singletonMap("s", null)));

    final ResultSet rs = database.query("opencypher",
        "MATCH (:R {id: 1})-[r:REL]->(:R {id: 2}) RETURN r.s AS s, keys(r) AS k");
    final var row = rs.next();
    assertThat(row.<Object>getProperty("s")).isNull();
    assertThat(row.<List<String>>getProperty("k")).doesNotContain("s");
  }

  /**
   * A side effect worth pinning explicitly: before this PR, {@code SetClauseApplier}'s private validation (#6863)
   * rejected any {@code Map} unconditionally, including the plain map {@code point()} returns internally - so
   * {@code SET n.loc = point(...)} threw {@code TypeError: InvalidPropertyType} on {@code main} today, the mirror
   * image of the CREATE/MERGE gap this PR closes. Now that {@code SetClauseApplier} delegates to the same
   * {@code CypherValues.coerceAndValidatePropertyValue}/{@code isPointShaped}, SET accepts a Point-valued property
   * exactly like CREATE and MERGE do.
   */
  @Test
  void setAcceptsAPointValuedPropertyLikeCreateAndMerge() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:R {id: 1})"));

    database.transaction(() -> database.command("opencypher",
        "MATCH (n:R {id: 1}) SET n.loc = point({longitude: 12.5, latitude: 55.6})"));

    final ResultSet dotAssign = database.query("opencypher", "MATCH (n:R {id: 1}) RETURN n.loc AS loc");
    assertThat(((Map<?, ?>) dotAssign.next().<Object>getProperty("loc")).get("latitude")).isEqualTo(55.6);

    database.transaction(() -> database.command("opencypher",
        "MATCH (n:R {id: 1}) SET n += {loc2: point({x: 1, y: 2})}"));

    final ResultSet mergeMap = database.query("opencypher", "MATCH (n:R {id: 1}) RETURN n.loc2 AS loc2");
    assertThat(((Map<?, ?>) mergeMap.next().<Object>getProperty("loc2")).get("x")).isEqualTo(1.0);
  }

  /**
   * The exact reported repro: {@code SET n += $p} with a parameter map whose own value is itself a map. This is the
   * behaviour that reads as a regression against CREATE without the two tests above - it was already correct.
   */
  @Test
  void setStillRejectsAMapValuedParameterPropertyMatchingCreate() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:R {id: 1})"));

    final Map<String, Object> mapValue = Map.of("k", 1, "nested", Map.of("deep", true));
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (n:R {id: 1}) SET n += $p", Map.of("p", Map.of("m", mapValue)))))
        .rootCause()
        .hasMessageContaining("TypeError: InvalidPropertyType");

    // Scalars in the same parameter map still work, matching the reported repro's earlier successful step.
    database.transaction(() -> database.command("opencypher",
        "MATCH (n:R {id: 1}) SET n += $p", Map.of("p", Map.of("s", "ok"))));
    final ResultSet rs = database.query("opencypher", "MATCH (n:R {id: 1}) RETURN n.s AS s");
    assertThat(rs.next().<String>getProperty("s")).isEqualTo("ok");
  }
}
