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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.function.procedure.Procedure;
import com.arcadedb.function.procedure.ProcedureRegistry;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11438: the Cypher procedures {@code db.analyticalView.create/list/rebuild/drop} manage a Graph Analytical View
 * without leaving Cypher, with the same effect as the SQL statements, whose bodies they share.
 */
class CypherAnalyticalViewProceduresTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("City");
    database.getSchema().createEdgeType("ROAD");
    database.getSchema().createDocumentType("Note");
    database.transaction(() -> {
      final MutableVertex rome = database.newVertex("City").set("name", "Rome").save();
      final MutableVertex milan = database.newVertex("City").set("name", "Milan").save();
      final MutableVertex turin = database.newVertex("City").set("name", "Turin").save();
      rome.newEdge("ROAD", milan, "distance", 570);
      milan.newEdge("ROAD", turin, "distance", 140);
    });
  }

  @Test
  void proceduresAreRegisteredWithTheirWriteFlag() {
    assertThat(procedure("db.analyticalView.create").isWriteProcedure()).isTrue();
    assertThat(procedure("db.analyticalView.rebuild").isWriteProcedure()).isTrue();
    assertThat(procedure("db.analyticalView.drop").isWriteProcedure()).isTrue();
    assertThat(procedure("db.analyticalView.list").isWriteProcedure()).isFalse();
    assertThat(((CypherProcedure) procedure("db.analyticalView.create")).isSchemaProcedure()).isTrue();
    assertThat(((CypherProcedure) procedure("db.analyticalView.rebuild")).isSchemaProcedure()).isTrue();
    assertThat(((CypherProcedure) procedure("db.analyticalView.drop")).isSchemaProcedure()).isTrue();
    assertThat(((CypherProcedure) procedure("db.analyticalView.list")).isSchemaProcedure()).isFalse();
  }

  /**
   * A write procedure changes the schema, so analyze() reports it as DDL with the SCHEMA operation type, exactly as the SQL
   * statements: an HA replica forwards it to the leader even inside a client transaction (it cannot write the schema itself),
   * and an operation-type gate (the MCP execute_command tool) asks for the schema permission, not the CRUD ones.
   */
  @Test
  void writeProceduresAreAnalyzedAsSchemaChangesLikeTheSqlStatements() {
    final QueryEngine cypher = database.getQueryEngine("opencypher");
    for (final String call : List.of("CALL db.analyticalView.create('roads', ['City'], ['ROAD'])",
        "CALL db.analyticalView.rebuild('roads')", "CALL db.analyticalView.drop('roads', {ifExists: true})")) {
      final QueryEngine.AnalyzedQuery analyzed = cypher.analyze(call);
      assertThat(analyzed.isDDL()).as(call).isTrue();
      assertThat(analyzed.isIdempotent()).as(call).isFalse();
      assertThat(analyzed.getOperationTypes()).as(call).containsExactly(OperationType.SCHEMA);
    }
    for (final String sql : List.of("CREATE GRAPH ANALYTICAL VIEW roads VERTEX TYPES (City) EDGE TYPES (ROAD)",
        "REBUILD GRAPH ANALYTICAL VIEW roads", "DROP GRAPH ANALYTICAL VIEW IF EXISTS roads")) {
      final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("sql").analyze(sql);
      assertThat(analyzed.isDDL()).as(sql).isTrue();
      assertThat(analyzed.getOperationTypes()).as(sql).containsExactly(OperationType.SCHEMA);
    }

    final QueryEngine.AnalyzedQuery list = cypher.analyze("CALL db.analyticalView.list()");
    assertThat(list.isDDL()).isFalse();
    assertThat(list.isIdempotent()).isTrue();
    assertThat(list.getOperationTypes()).containsExactly(OperationType.READ);

    // A data write next to the schema change keeps its own operation type besides SCHEMA
    final QueryEngine.AnalyzedQuery mixed = cypher.analyze(
        "CREATE (:City {name: 'Naples'}) WITH 1 AS one CALL db.analyticalView.create('roads') YIELD name RETURN name");
    assertThat(mixed.isDDL()).isTrue();
    assertThat(mixed.getOperationTypes()).containsExactlyInAnyOrder(OperationType.SCHEMA, OperationType.CREATE);

    // A UNION branch carrying the call makes the whole statement a schema change
    final QueryEngine.AnalyzedQuery union = cypher.analyze(
        "CALL db.analyticalView.list() YIELD name RETURN name UNION CALL db.analyticalView.drop('roads') YIELD name RETURN name");
    assertThat(union.isDDL()).isTrue();
    assertThat(union.getOperationTypes()).contains(OperationType.SCHEMA);
  }

  @Test
  void createPersistsTheDefinitionWithEveryConfigKey() {
    final Result row = single(database.command("opencypher",
        "CALL db.analyticalView.create('roads', ['City'], ['ROAD'], "
            + "{properties: ['name'], edgeProperties: ['distance'], updateMode: 'SYNCHRONOUS', compactionThreshold: 500, cch: ['distance']})"));
    assertThat(row.<String>getProperty("name")).isEqualTo("roads");
    assertThat(row.<Boolean>getProperty("created")).isTrue();
    assertThat(row.<String>getProperty("status")).isIn("BUILDING", "READY");

    final JSONObject definition = definition("roads");
    assertThat(definition.getJSONArray("vertexTypes").toList()).containsExactly("City");
    assertThat(definition.getJSONArray("edgeTypes").toList()).containsExactly("ROAD");
    assertThat(definition.getJSONArray("propertyFilter").toList()).containsExactly("name");
    assertThat(definition.getJSONArray("edgePropertyFilter").toList()).contains("distance");
    assertThat(definition.getString("updateMode")).isEqualTo("SYNCHRONOUS");
    assertThat(definition.getInt("compactionThreshold")).isEqualTo(500);
    assertThat(definition.getJSONArray("contractionHierarchies").getJSONObject(0).getString("weightProperty")).isEqualTo("distance");

    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "roads");
    assertThat(view).isNotNull();
    assertThat(view.awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(view.getNodeCount()).isEqualTo(3);
    assertThat(view.getEdgeCount()).isEqualTo(2);
  }

  @Test
  void createWithNameOnlyCoversEveryType() {
    final Result row = single(database.command("opencypher", "CALL db.analyticalView.create('all')"));
    assertThat(row.<Boolean>getProperty("created")).isTrue();
    final JSONObject definition = definition("all");
    assertThat(definition.has("vertexTypes")).isFalse();
    assertThat(definition.has("edgeTypes")).isFalse();
    assertThat(definition.has("propertyFilter")).isFalse();
    assertThat(definition.getString("updateMode")).isEqualTo("OFF");
  }

  @Test
  void anEmptyPropertiesListCopiesNoVertexProperty() {
    database.command("opencypher", "CALL db.analyticalView.create('bare', null, null, {properties: []})").close();
    // As SQL PROPERTIES (): an empty filter, not an absent one (issue #9831)
    assertThat(definition("bare").getJSONArray("propertyFilter").length()).isEqualTo(0);
  }

  @Test
  void createMatchesTheSqlStatementDefinition() {
    database.command("sql",
        "CREATE GRAPH ANALYTICAL VIEW viaSql VERTEX TYPES (City) EDGE TYPES (ROAD) PROPERTIES (name) UPDATE MODE ASYNCHRONOUS").close();
    database.command("opencypher",
        "CALL db.analyticalView.create('viaCypher', 'City', 'ROAD', {properties: ['name'], updateMode: 'asynchronous'})").close();

    final JSONObject sql = definition("viaSql");
    final JSONObject cypher = definition("viaCypher");
    sql.remove("name");
    cypher.remove("name");
    assertThat(cypher.toMap()).isEqualTo(sql.toMap());
  }

  @Test
  void createOnAnExistingNameIsRefusedUnlessIfNotExists() {
    database.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['ROAD'])").close();

    assertThatThrownBy(() -> database.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['ROAD'])").close())
        .hasRootCauseInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("roads");

    final Result row = single(
        database.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['ROAD'], {ifNotExists: true})"));
    assertThat(row.<Boolean>getProperty("created")).isFalse();
    assertThat(row.<String>getProperty("status")).isIn("BUILDING", "READY");
  }

  @Test
  void unknownConfigKeyIsRefusedByName() {
    assertThatThrownBy(() -> database.command("opencypher",
        "CALL db.analyticalView.create('roads', ['City'], ['ROAD'], {updateMod: 'SYNCHRONOUS'})").close())
        .hasMessageContaining("updateMod");
    assertThat(database.getSchema().getExtension("graphAnalyticalViews")).isNull();
  }

  @Test
  void unknownUpdateModeIsRefused() {
    assertThatThrownBy(() -> database.command("opencypher",
        "CALL db.analyticalView.create('roads', ['City'], ['ROAD'], {updateMode: 'SOMETIMES'})").close())
        .hasMessageContaining("SOMETIMES");
    assertThat(database.getSchema().getExtension("graphAnalyticalViews")).isNull();
  }

  @Test
  void negativeCompactionThresholdIsRefused() {
    assertThatThrownBy(() -> database.command("opencypher",
        "CALL db.analyticalView.create('roads', ['City'], ['ROAD'], {compactionThreshold: -5})").close())
        .hasMessageContaining("compactionThreshold");
    assertThat(database.getSchema().getExtension("graphAnalyticalViews")).isNull();
  }

  @Test
  void unknownOrWrongKindTypeNamesAreRefusedByName() {
    assertThatThrownBy(() -> database.command("opencypher", "CALL db.analyticalView.create('roads', ['Citty'], ['ROAD'])").close())
        .hasMessageContaining("Citty");
    assertThatThrownBy(() -> database.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['ROADS'])").close())
        .hasMessageContaining("ROADS");
    // A vertex type where an edge type goes, and a document type where a vertex type goes
    assertThatThrownBy(() -> database.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['City'])").close())
        .hasMessageContaining("City");
    assertThatThrownBy(() -> database.command("opencypher", "CALL db.analyticalView.create('roads', ['Note'], ['ROAD'])").close())
        .hasMessageContaining("Note");
    assertThat(database.getSchema().getExtension("graphAnalyticalViews")).isNull();
  }

  @Test
  void blankNameIsRefused() {
    assertThatThrownBy(() -> database.command("opencypher", "CALL db.analyticalView.create('  ')").close())
        .hasMessageContaining("name");
    assertThatThrownBy(() -> database.command("opencypher", "CALL db.analyticalView.drop(null)").close())
        .hasMessageContaining("name");
  }

  @Test
  void listYieldsTheSameFiguresAsTheSchemaQuery() {
    database.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['ROAD'], {updateMode: 'SYNCHRONOUS'})").close();
    database.command("opencypher", "CALL db.analyticalView.create('all')").close();
    assertThat(GraphAnalyticalViewRegistry.get(database, "roads").awaitReady(30, TimeUnit.SECONDS)).isTrue();
    assertThat(GraphAnalyticalViewRegistry.get(database, "all").awaitReady(30, TimeUnit.SECONDS)).isTrue();

    final List<Result> cypher = all(database.query("opencypher", "CALL db.analyticalView.list()"));
    final List<Result> sql = all(database.query("sql", "SELECT FROM schema:graphAnalyticalViews"));
    assertThat(cypher).hasSize(2);
    assertThat(cypher.stream().map(r -> r.<String>getProperty("name")).toList()).containsExactly("all", "roads");
    for (int i = 0; i < sql.size(); i++)
      for (final String field : sql.get(i).getPropertyNames())
        assertThat(cypher.get(i).<Object>getProperty(field)).as(field).isEqualTo(sql.get(i).getProperty(field));

    final Result roads = cypher.get(1);
    assertThat(roads.<String>getProperty("status")).isEqualTo("READY");
    assertThat(roads.<String>getProperty("updateMode")).isEqualTo("SYNCHRONOUS");
    assertThat(roads.<Number>getProperty("nodeCount").longValue()).isEqualTo(3);
    assertThat(roads.<Number>getProperty("edgeCount").longValue()).isEqualTo(2);
    assertThat(roads.<Number>getProperty("memoryUsageBytes").longValue()).isPositive();
    assertThat(roads.<List<String>>getProperty("vertexTypes")).containsExactly("City");
  }

  @Test
  void listYieldsEveryDeclaredFieldOnEveryRow() {
    database.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['ROAD'])").close();
    final List<Result> rows = all(database.query("opencypher",
        "CALL db.analyticalView.list() YIELD name, status, nodeCount, edgeCount, memoryUsageBytes, buildError "
            + "RETURN name, status, nodeCount, edgeCount, memoryUsageBytes, buildError"));
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<String>getProperty("name")).isEqualTo("roads");
    assertThat(rows.getFirst().getPropertyNames()).contains("buildError");
  }

  @Test
  void listOnADatabaseWithoutViewsYieldsNothing() {
    assertThat(all(database.query("opencypher", "CALL db.analyticalView.list()"))).isEmpty();
  }

  @Test
  void rebuildReportsTheStatusAndRefusesAnUnknownName() {
    database.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['ROAD'])").close();
    assertThat(GraphAnalyticalViewRegistry.get(database, "roads").awaitReady(30, TimeUnit.SECONDS)).isTrue();

    database.transaction(() -> {
      final MutableVertex naples = database.newVertex("City").set("name", "Naples").save();
      naples.newEdge("ROAD", database.query("sql", "SELECT FROM City WHERE name = 'Rome'").next().getVertex().get(), "distance", 230);
    });

    final Result row = single(database.command("opencypher", "CALL db.analyticalView.rebuild('roads')"));
    assertThat(row.<String>getProperty("name")).isEqualTo("roads");
    // The SQL REBUILD builds synchronously, and so does the procedure
    assertThat(row.<String>getProperty("status")).isEqualTo("READY");
    assertThat(GraphAnalyticalViewRegistry.get(database, "roads").getNodeCount()).isEqualTo(4);

    assertThatThrownBy(() -> database.command("opencypher", "CALL db.analyticalView.rebuild('nope')").close())
        .hasRootCauseInstanceOf(SchemaException.class)
        .hasMessageContaining("nope");
  }

  @Test
  void dropRemovesTheViewAndHonorsIfExists() {
    database.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['ROAD'])").close();

    final Result row = single(database.command("opencypher", "CALL db.analyticalView.drop('roads')"));
    assertThat(row.<String>getProperty("name")).isEqualTo("roads");
    assertThat(row.<Boolean>getProperty("dropped")).isTrue();
    assertThat(GraphAnalyticalViewRegistry.get(database, "roads")).isNull();
    assertThat(database.getSchema().getExtension("graphAnalyticalViews")).isNull();

    assertThatThrownBy(() -> database.command("opencypher", "CALL db.analyticalView.drop('roads')").close())
        .hasRootCauseInstanceOf(SchemaException.class)
        .hasMessageContaining("roads");

    final Result missing = single(database.command("opencypher", "CALL db.analyticalView.drop('roads', {ifExists: true})"));
    assertThat(missing.<Boolean>getProperty("dropped")).isFalse();

    assertThatThrownBy(() -> database.command("opencypher", "CALL db.analyticalView.drop('roads', {ifExist: true})").close())
        .hasMessageContaining("ifExist");
  }

  @Test
  void aViewCreatedInCypherIsDroppedBySqlAndViceVersa() {
    database.command("opencypher", "CALL db.analyticalView.create('a', ['City'], ['ROAD'])").close();
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW a").close();
    assertThat(GraphAnalyticalViewRegistry.get(database, "a")).isNull();

    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW b VERTEX TYPES (City) EDGE TYPES (ROAD)").close();
    database.command("opencypher", "CALL db.analyticalView.drop('b')").close();
    assertThat(GraphAnalyticalViewRegistry.get(database, "b")).isNull();
    assertThat(database.getSchema().getExtension("graphAnalyticalViews")).isNull();
  }

  @Test
  void aRolledBackCallerTransactionLeavesTheViewAndItsDefinitionTogether() {
    // The definition is a schema change, which a transaction does not hold: a rollback of the caller's transaction keeps it,
    // together with the live view, rather than leaving a live view without its definition
    database.begin();
    database.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['ROAD'])").close();
    database.rollback();

    assertThat(GraphAnalyticalViewRegistry.get(database, "roads")).isNotNull();
    assertThat(definitionOrNull("roads")).isNotNull();
    assertThat(all(database.query("opencypher", "CALL db.analyticalView.list()"))).hasSize(1);
  }

  @Test
  void aViewCreatedByTheProcedureServesCypher() {
    database.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['ROAD'])").close();
    assertThat(GraphAnalyticalViewRegistry.get(database, "roads").awaitReady(30, TimeUnit.SECONDS)).isTrue();
    final Result row = single(database.query("opencypher", "MATCH (:City)-[:ROAD]->(b:City) RETURN count(b) AS n"));
    assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(2);
  }

  private static Procedure procedure(final String name) {
    final Procedure procedure = ProcedureRegistry.get(name);
    assertThat(procedure).as(name).isNotNull();
    return procedure;
  }

  private JSONObject definition(final String name) {
    final JSONObject definition = definitionOrNull(name);
    assertThat(definition).as(name).isNotNull();
    return definition;
  }

  private JSONObject definitionOrNull(final String name) {
    final JSONObject all = database.getSchema().getExtension("graphAnalyticalViews");
    return all != null && all.has(name) ? all.getJSONObject(name) : null;
  }

  private static Result single(final ResultSet resultSet) {
    final List<Result> rows = all(resultSet);
    assertThat(rows).hasSize(1);
    return rows.getFirst();
  }

  private static List<Result> all(final ResultSet resultSet) {
    final List<Result> rows = new ArrayList<>();
    try (resultSet) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }

  /**
   * The three write procedures need {@code UPDATE_SCHEMA}, as the SQL statements do; listing needs none, as
   * {@code SELECT FROM schema:graphAnalyticalViews} needs none. On a database with security, which {@link TestHelper}'s is not.
   */
  @Nested
  class SchemaPermission {
    private static final String PATH = "target/databases/CypherAnalyticalViewProceduresTest-SchemaPermission";

    private DatabaseFactory securedFactory;
    private Database        secured;

    @BeforeEach
    void setUp() {
      securedFactory = new DatabaseFactory(PATH).setSecurity(db -> {
      });
      if (securedFactory.exists())
        securedFactory.open().drop();
      secured = securedFactory.create();
      secured.getSchema().createVertexType("City");
      secured.getSchema().createEdgeType("ROAD");
      secured.command("opencypher", "CALL db.analyticalView.create('roads', ['City'], ['ROAD'])").close();
    }

    @AfterEach
    void tearDown() {
      if (secured != null && secured.isOpen()) {
        DatabaseContext.INSTANCE.getContext(secured.getDatabasePath()).setCurrentUser(null);
        secured.drop();
      }
      securedFactory.close();
    }

    @Test
    void writeProceduresNeedSchemaPermissionListDoesNot() {
      bindUserWithoutSchemaPermission();

      assertRefused("CALL db.analyticalView.create('other', ['City'], ['ROAD'])");
      assertRefused("CALL db.analyticalView.rebuild('roads')");
      assertRefused("CALL db.analyticalView.drop('roads')");

      // Refused before anything changed
      final JSONObject all = secured.getSchema().getExtension("graphAnalyticalViews");
      assertThat(all.has("other")).isFalse();
      assertThat(all.has("roads")).isTrue();
      assertThat(GraphAnalyticalViewRegistry.get(secured, "roads")).isNotNull();

      assertThat(all(secured.query("opencypher", "CALL db.analyticalView.list()"))).hasSize(1);
    }

    private void assertRefused(final String cypher) {
      assertThatThrownBy(() -> secured.command("opencypher", cypher).close())
          .as(cypher)
          .hasRootCauseInstanceOf(SecurityException.class)
          .hasMessageContaining("not allowed to update schema");
    }

    private void bindUserWithoutSchemaPermission() {
      DatabaseContext.INSTANCE.getContext(secured.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
        @Override
        public String getName() {
          return "reader";
        }

        @Override
        public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
          return false;
        }

        @Override
        public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
          return true;
        }

        @Override
        public boolean requestAccessOnType(final String typeName, final ACCESS access) {
          return true;
        }

        @Override
        public long getResultSetLimit() {
          return -1L;
        }

        @Override
        public long getReadTimeout() {
          return -1L;
        }
      });
    }
  }
}
