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
package com.arcadedb.query.opencypher.procedures.refactor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the apoc.refactor.cloneNodesWithRelationships Cypher procedure
 * (registered as "refactor.cloneNodesWithRelationships").
 * <p>
 * The procedure mutates the graph, so every CALL runs inside an explicit transaction that is fully
 * consumed and committed before any assertion - see {@link RefactorMergeNodesTest} class Javadoc for
 * why CypherProcedure execution needs that.
 * </p>
 */
class RefactorCloneNodesWithRelationshipsTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-refactor-clone-nodes");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void registeredUnderBothPlainAndApocPrefixedName() {
    assertThat(CypherProcedureRegistry.hasProcedure("refactor.cloneNodesWithRelationships")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.refactor.cloneNodesWithRelationships")).isTrue();
    assertThat(CypherProcedureRegistry.get("apoc.refactor.cloneNodesWithRelationships"))
        .isSameAs(CypherProcedureRegistry.get("refactor.cloneNodesWithRelationships"));
  }

  @Test
  void clonesNodeWithSameTypeAndProperties() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("age", 30L).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL apoc.refactor.cloneNodesWithRelationships([a], {}) YIELD input, output, error RETURN input, output, error");
    final Result result = rs.next();
    final Object error = result.getProperty("error");
    final Vertex input = result.getProperty("input");
    final Vertex output = result.getProperty("output");
    database.commit();

    assertThat(error).isNull();
    assertThat(output.getIdentity()).isNotEqualTo(input.getIdentity());
    assertThat(output.getTypeName()).isEqualTo("Person");
    assertThat(output.getString("name")).isEqualTo("A");
    assertThat(output.getLong("age")).isEqualTo(30L);
  }

  @Test
  void clonedRelationshipToExternalNodePointsAtOriginalExternalNode() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex external = database.newVertex("Person").set("name", "External").save();
    a.newEdge("KNOWS", external, "since", 2020L).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL apoc.refactor.cloneNodesWithRelationships([a], {}) YIELD output RETURN output");
    final Vertex clone = rs.next().getProperty("output");
    database.commit();

    final ResultSet edges = database.query("opencypher",
        "MATCH (c:Person)-[r:KNOWS]->(e:Person {name:'External'}) WHERE id(c) = $cloneId RETURN r.since AS since",
        java.util.Map.of("cloneId", clone.getIdentity().toString()));
    assertThat(edges.hasNext()).isTrue();
    assertThat(edges.next().<Long>getProperty("since")).isEqualTo(2020L);

    // original relationship must be untouched
    final ResultSet originalEdges = database.query("opencypher",
        "MATCH (a:Person {name:'A'})-[r:KNOWS]->(e:Person {name:'External'}) RETURN r");
    assertThat(originalEdges.hasNext()).isTrue();
  }

  @Test
  void relationshipBetweenTwoClonedNodesConnectsTheTwoClones() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    a.newEdge("KNOWS", b, "since", 2021L).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.cloneNodesWithRelationships([a,b], {}) YIELD input, output RETURN input, output");
    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());
    database.commit();

    Vertex cloneOfA = null;
    Vertex cloneOfB = null;
    for (final Result row : rows) {
      final Vertex input = row.getProperty("input");
      final Vertex output = row.getProperty("output");
      if ("A".equals(input.getString("name")))
        cloneOfA = output;
      else if ("B".equals(input.getString("name")))
        cloneOfB = output;
    }

    assertThat(cloneOfA).isNotNull();
    assertThat(cloneOfB).isNotNull();

    boolean cloneToCloneEdgeFound = false;
    for (final com.arcadedb.graph.Edge e : cloneOfA.getEdges(Vertex.DIRECTION.OUT, "KNOWS")) {
      if (e.getIn().equals(cloneOfB.getIdentity())) {
        cloneToCloneEdgeFound = true;
        assertThat(e.getLong("since")).isEqualTo(2021L);
      }
    }
    assertThat(cloneToCloneEdgeFound).isTrue();

    // original relationship between a and b must be untouched
    final ResultSet originalEdges = database.query("opencypher",
        "MATCH (a:Person {name:'A'})-[r:KNOWS]->(b:Person {name:'B'}) RETURN r");
    assertThat(originalEdges.hasNext()).isTrue();
  }

  @Test
  void duplicateNodeInTheListProducesOneCloneWithAllItsEdges() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex external = database.newVertex("Person").set("name", "External").save();
    a.newEdge("KNOWS", external, "since", 2020L).save();
    database.commit();

    // Before nodes were deduplicated, [a,a] created TWO clones but cloneOf (keyed by the original's
    // identity) only remembered the second, so the edge phase wired every one of 'a's edges onto that
    // second clone while the first clone - still yielded with error=null - silently got none.
    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL apoc.refactor.cloneNodesWithRelationships([a,a], {}) "
            + "YIELD input, output, error RETURN input, output, error");
    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());
    database.commit();

    assertThat(rows).hasSize(1);
    final Vertex output = rows.get(0).getProperty("output");

    final ResultSet clonedEdges = database.query("opencypher",
        "MATCH (c:Person)-[r:KNOWS]->(e:Person {name:'External'}) WHERE id(c) = $cloneId RETURN r.since AS since",
        java.util.Map.of("cloneId", output.getIdentity().toString()));
    assertThat(clonedEdges.hasNext()).isTrue();
    assertThat(clonedEdges.next().<Long>getProperty("since")).isEqualTo(2020L);
  }

  @Test
  void nonMapConfigThrows() {
    database.begin();
    database.newVertex("Person").set("name", "A").save();
    database.commit();

    assertThatThrownBy(() -> database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL apoc.refactor.cloneNodesWithRelationships([a], 'not-a-map') YIELD output RETURN output").hasNext())
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void skipPropertiesExcludesGivenPropertyFromTheClone() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("secret", "s3cr3t").save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}) "
            + "CALL apoc.refactor.cloneNodesWithRelationships([a], {skipProperties: ['secret']}) YIELD output RETURN output");
    final Vertex output = rs.next().getProperty("output");
    database.commit();

    assertThat(output.getString("name")).isEqualTo("A");
    assertThat(output.getPropertyNames()).doesNotContain("secret");
  }

  @Test
  void nodeCloneFailureProducesPopulatedErrorFieldWithoutLosingOtherRows() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").set("requiredTag", "present").save();
    database.newVertex("Person").set("name", "B").save();
    database.commit();

    // A mandatory-property constraint is validated synchronously on save() (LocalDatabase.createRecordNoLock
    // calls MutableDocument.validate() before the record is written), unlike a UNIQUE index violation, which
    // ArcadeDB defers to commit time - so this is the reliable way to force RefactorCloneNodesWithRelationships
    // .execute()'s per-node try/catch to actually fire. Both 'A' and 'B' predate the constraint, so it is only
    // enforced against the NEW record the clone creates: 'A' copies a value for it and clones fine, 'B' never
    // had one set and fails clone validation.
    database.getSchema().getType("Person").createProperty("requiredTag", String.class).setMandatory(true);

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) "
            + "CALL apoc.refactor.cloneNodesWithRelationships([a,b], {}) YIELD input, output, error RETURN input, output, error");
    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());
    database.commit();

    assertThat(rows).hasSize(2);
    Result rowForA = null;
    Result rowForB = null;
    for (final Result row : rows) {
      final Vertex input = row.getProperty("input");
      if (a.getIdentity().equals(input.getIdentity()))
        rowForA = row;
      else
        rowForB = row;
    }

    final Object errorForA = rowForA.getProperty("error");
    final Vertex outputForA = rowForA.getProperty("output");
    assertThat(errorForA).isNull();
    assertThat(outputForA).isNotNull();

    final Object outputForB = rowForB.getProperty("output");
    final String errorForB = rowForB.getProperty("error");
    assertThat(outputForB).isNull();
    assertThat(errorForB).isNotBlank();
  }

  @Test
  void edgeCloneFailureIsSkippedWithoutLosingTheNodeYieldRow() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex external = database.newVertex("Person").set("name", "External").save();
    a.newEdge("KNOWS", external, (Object[]) null).save();
    database.commit();

    // Same mandatory-property mechanism as nodeCloneFailureProducesPopulatedErrorFieldWithoutLosingOtherRows
    // above, applied to the edge-clone phase: the original edge predates the constraint and never set the
    // property, so cloning it (which only copies properties the original actually has) creates a new edge
    // missing a now-mandatory property, failing validation synchronously in cloneEdge()'s try/catch.
    database.getSchema().getType("KNOWS").createProperty("requiredCode", String.class).setMandatory(true);

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL apoc.refactor.cloneNodesWithRelationships([a], {}) "
            + "YIELD input, output, error RETURN input, output, error");
    final Result result = rs.next();
    final Object error = result.getProperty("error");
    final Vertex output = result.getProperty("output");
    database.commit();

    // the node row survives even though its only edge failed to clone
    assertThat(error).isNull();
    assertThat(output).isNotNull();

    final ResultSet clonedEdges = database.query("opencypher",
        "MATCH (c:Person)-[r:KNOWS]->(e:Person {name:'External'}) WHERE id(c) = $cloneId RETURN r",
        java.util.Map.of("cloneId", output.getIdentity().toString()));
    assertThat(clonedEdges.hasNext()).isFalse();
  }

  /**
   * Issue #7427: APOC declares {@code apoc.refactor.cloneNodesWithRelationships(nodes :: LIST<NODE>, config = {} ::
   * MAP)}, so code migrated from Neo4j routinely omits the config. The procedure used to declare the config
   * mandatory and rejected that call outright.
   */
  @Test
  void clonesNodeWithItsRelationshipsWhenTheConfigArgumentIsOmitted() {
    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").set("age", 30L).save();
    final MutableVertex first = database.newVertex("Person").set("name", "First").save();
    final MutableVertex second = database.newVertex("Person").set("name", "Second").save();
    a.newEdge("KNOWS", first).save();
    a.newEdge("KNOWS", second).save();
    database.commit();

    database.begin();
    final ResultSet rs = database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL apoc.refactor.cloneNodesWithRelationships([a]) YIELD input, output, error "
            + "RETURN input, output, error");
    final Result result = rs.next();
    final Object error = result.getProperty("error");
    final Vertex input = result.getProperty("input");
    final Vertex output = result.getProperty("output");
    database.commit();

    assertThat(error).isNull();
    assertThat(output.getIdentity()).isNotEqualTo(input.getIdentity());
    assertThat(output.getTypeName()).isEqualTo("Person");
    assertThat(output.getString("name")).isEqualTo("A");
    assertThat(output.getLong("age")).isEqualTo(30L);

    final ResultSet neighbours = database.query("opencypher",
        "MATCH (c:Person)-[:KNOWS]->(n:Person) WHERE id(c) = $cloneId RETURN n.name AS name ORDER BY name",
        Map.of("cloneId", output.getIdentity().toString()));
    final List<String> names = new ArrayList<>();
    while (neighbours.hasNext())
      names.add(neighbours.next().getProperty("name"));
    assertThat(names).containsExactly("First", "Second");
  }

  /**
   * Issue #7427: omitting the config must be indistinguishable from passing {@code {}} - including for the
   * plain, non-{@code apoc.}-prefixed name, which resolves to the same instance.
   */
  @Test
  void omittedConfigIsIndistinguishableFromAnEmptyMap() {
    database.begin();
    database.newVertex("Person").set("name", "A").set("secret", "s").save();
    database.commit();

    database.begin();
    final ResultSet omitted = database.command("opencypher",
        "MATCH (a:Person {name:'A'}) CALL refactor.cloneNodesWithRelationships([a]) YIELD output RETURN output");
    final Vertex cloneWithoutConfig = omitted.next().getProperty("output");
    database.commit();

    database.begin();
    final ResultSet explicit = database.command("opencypher",
        "MATCH (a:Person {name:'A'}) WHERE a.secret = 's' CALL refactor.cloneNodesWithRelationships([a], {}) "
            + "YIELD output RETURN output");
    final Vertex cloneWithEmptyConfig = explicit.next().getProperty("output");
    database.commit();

    assertThat(cloneWithoutConfig.getPropertyNames()).isEqualTo(cloneWithEmptyConfig.getPropertyNames());
    assertThat(cloneWithoutConfig.getString("secret")).isEqualTo("s");
    assertThat(cloneWithEmptyConfig.getString("secret")).isEqualTo("s");
  }

  /**
   * Issue #7427, the direct-caller entry point: {@code execute()} is public, so the arity gate every caller passes
   * through is {@code validateArgs}. It has to accept 1 and 2 arguments and keep rejecting 0 and 3, with the
   * declared bounds appearing in the message.
   */
  @Test
  void arityGateAcceptsOneOrTwoArgumentsAndStillRejectsTheRest() {
    final CypherProcedure procedure = CypherProcedureRegistry.get("apoc.refactor.cloneNodesWithRelationships");

    assertThat(procedure.getMinArgs()).isEqualTo(1);
    assertThat(procedure.getMaxArgs()).isEqualTo(2);

    assertThatCode(() -> procedure.validateArgs(new Object[] { List.of() })).doesNotThrowAnyException();
    assertThatCode(() -> procedure.validateArgs(new Object[] { List.of(), Map.of() })).doesNotThrowAnyException();

    assertThatThrownBy(() -> procedure.validateArgs(new Object[] {}))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("expects 1-2 arguments but got 0");
    assertThatThrownBy(() -> procedure.validateArgs(new Object[] { List.of(), Map.of(), Map.of() }))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("expects 1-2 arguments but got 3");
  }
}
