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
package com.arcadedb.bolt;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.opencypher.procedures.db.DbLabels;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #6151: the schema procedures the Bolt executor answers by itself
 * ({@code db.labels()}, {@code db.relationshipTypes()}, {@code db.propertyKeys()}) must return exactly what
 * the native Cypher {@code CALL} path returns, because both now run the same
 * {@link com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry} entries.
 * <p>
 * Every assertion here compares the Bolt answer against the engine's answer for the same call rather than
 * against a hardcoded expectation, so it stays true when a procedure's own semantics change. The fixture is
 * built so the two implementations that used to exist disagree: an edge type carrying Cypher's composite
 * {@code ~} label separator (the Bolt copy listed it, the registry filters it out) and property names
 * declared out of alphabetical order (the Bolt copy sorted them, the registry keeps schema order).
 * </p>
 */
class Issue6151BoltSchemaProcedureTest {
  private static final String DB_PATH = "./target/databases/Issue6151BoltSchemaProcedureTest";

  /** The introspection query Neo4j Desktop sends: the three procedures unioned, each collected into a list. */
  private static final String COMBINED_QUERY =
      "CALL db.labels() YIELD label RETURN collect(label) AS result "
          + "UNION CALL db.relationshipTypes() YIELD relationshipType RETURN collect(relationshipType) AS result "
          + "UNION CALL db.propertyKeys() YIELD propertyKey RETURN collect(propertyKey) AS result";

  private Database db;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    db = factory.create();

    // Property names are declared in a deliberately non-alphabetical order: the old Bolt copy sorted them.
    db.getSchema().createVertexType("Zebra").createProperty("zeta", Type.STRING);
    db.getSchema().getType("Zebra").createProperty("alpha", Type.STRING);
    db.getSchema().createVertexType("Apple").createProperty("mango", Type.STRING);
    db.getSchema().createEdgeType("KNOWS").createProperty("since", Type.INTEGER);
    // A composite type, as the Cypher engine names one for a multi-labelled element: the registry filters
    // these out of both db.relationshipTypes() and db.propertyKeys(), the old Bolt copy only did for labels.
    db.getSchema().createEdgeType("KNOWS~WORKS_WITH").createProperty("ignored", Type.STRING);
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  @DisplayName("db.labels() over Bolt returns the engine's rows, field name included")
  void labelsMatchTheEngine() {
    assertServedQueryMatchesEngine("CALL db.labels()", "label");
  }

  @Test
  @DisplayName("db.relationshipTypes() over Bolt filters composite ~ types exactly like the engine")
  void relationshipTypesMatchTheEngine() {
    final List<Object> served = assertServedQueryMatchesEngine("CALL db.relationshipTypes() YIELD relationshipType",
        "relationshipType");
    // Guards the fixture: the composite edge type exists, so a run where nothing filtered it would differ.
    assertThat(db.getSchema().getTypes().stream().anyMatch(t -> t.getName().equals("KNOWS~WORKS_WITH"))).isTrue();
    assertThat(served).contains("KNOWS").doesNotContain("KNOWS~WORKS_WITH");
  }

  @Test
  @DisplayName("db.propertyKeys() over Bolt keeps the engine's order, not its own sorted one")
  void propertyKeysMatchTheEngine() {
    final List<Object> served = assertServedQueryMatchesEngine("CALL db.propertyKeys()", "propertyKey");
    // Guards the fixture: alphabetical order and schema order genuinely differ here.
    assertThat(served).containsSubsequence("zeta", "alpha");
  }

  @Test
  @DisplayName("the combined UNION query Neo4j Desktop sends collects the engine's values")
  void theCombinedDesktopQueryCollectsTheEngineValues() {
    final BoltSystemProcedures.Served served = serve(COMBINED_QUERY);
    assertThat(served).isNotNull();
    assertThat(served.fields()).containsExactly("result");
    assertThat(served.rows()).hasSize(3);

    assertThat(served.rows().get(0).getFirst()).isEqualTo(engineValues("CALL db.labels()", "label"));
    assertThat(served.rows().get(1).getFirst())
        .isEqualTo(engineValues("CALL db.relationshipTypes()", "relationshipType"));
    assertThat(served.rows().get(2).getFirst()).isEqualTo(engineValues("CALL db.propertyKeys()", "propertyKey"));
  }

  @Test
  @DisplayName("the combined query declines as a whole when any of its three calls carries an argument")
  void theCombinedQueryDeclinesWhenOneCallCarriesAnArgument() {
    final String query = "CALL db.labels() YIELD label RETURN collect(label) AS result "
        + "UNION CALL db.relationshipTypes('unexpected') YIELD relationshipType "
        + "RETURN collect(relationshipType) AS result "
        + "UNION CALL db.propertyKeys() YIELD propertyKey RETURN collect(propertyKey) AS result";

    // One bad call sends the whole batch to the engine: answering the other two here would hand back a
    // result set for a query the engine would have rejected.
    assertThat(serve(query)).isNull();
  }

  @Test
  @DisplayName("a call carrying arguments is left to the engine, which rejects it on arity")
  void aCallWithArgumentsIsLeftToTheEngine() {
    assertThat(serve("CALL db.labels('unexpected')")).isNull();
    assertThat(serve("CALL db.relationshipTypes(1)")).isNull();
    assertThat(serve("CALL db.propertyKeys( 'x' )")).isNull();

    // Whitespace between the name and its empty argument list is still a no-argument call.
    assertThat(serve("CALL db.labels ( )")).isNotNull();

    // What the Bolt connection then gets is the engine's own answer: the registry's arity error, the very
    // error every other client sees, instead of the full label list the interception used to hand back.
    assertThatThrownBy(() -> db.query("opencypher", "CALL db.labels('unexpected')"))
        .hasMessageContaining("db.labels");
  }

  @Test
  @DisplayName("no database selected yields no rows rather than failing")
  void withoutADatabaseTheAnswerIsEmpty() {
    final BoltSystemProcedures.Served served = BoltSystemProcedures.serveSchemaProcedure(null,
        BoltSystemProcedures.normalize("CALL db.labels()"));
    assertThat(served).isNotNull();
    assertThat(served.fields()).containsExactly("label");
    assertThat(served.rows()).isEmpty();
  }

  @Test
  @DisplayName("the combined query with no database selected yields no rows rather than failing")
  void theCombinedQueryWithoutADatabaseIsEmpty() {
    final BoltSystemProcedures.Served served = BoltSystemProcedures.serveSchemaProcedure(null,
        BoltSystemProcedures.normalize(COMBINED_QUERY));
    assertThat(served).isNotNull();
    assertThat(served.fields()).containsExactly("result");
    assertThat(served.rows()).isEmpty();
  }

  @Test
  @DisplayName("a procedure that blows up declines the query instead of escaping the interception")
  void aFailingProcedureLeavesTheQueryToTheEngine() {
    // The Bolt executor runs the interception before the try/catch that classifies query errors, so anything
    // escaping here would reach the connection loop unclassified. Declining sends the query to the engine.
    CypherProcedureRegistry.registerOrReplace(new ExplodingLabels());
    try {
      assertThat(serve("CALL db.labels()")).isNull();
      assertThat(serve(COMBINED_QUERY)).isNull();
    } finally {
      CypherProcedureRegistry.registerOrReplace(new DbLabels());
    }

    // Restored: the same call is served again.
    assertThat(serve("CALL db.labels()")).isNotNull();
  }

  @Test
  @DisplayName("procedure-name matching survives a locale whose lowercasing rewrites I")
  void matchingIsLocaleIndependent() {
    final Locale original = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      final String normalized = BoltSystemProcedures.normalize("CALL DB.RELATIONSHIPTYPES()");
      assertThat(normalized).isEqualTo("call db.relationshiptypes()");
      assertThat(BoltSystemProcedures.isSchemaProcedureQuery(normalized)).isTrue();
    } finally {
      Locale.setDefault(original);
    }
  }

  /**
   * Serves the query through the Bolt path and asserts it carries the engine's field name and values.
   *
   * @return the served values, flattened, for further fixture assertions
   */
  private List<Object> assertServedQueryMatchesEngine(final String query, final String field) {
    final BoltSystemProcedures.Served served = serve(query);
    assertThat(served).isNotNull();
    assertThat(served.fields()).containsExactly(field);

    final List<Object> values = new ArrayList<>(served.rows().size());
    for (final List<Object> row : served.rows()) {
      assertThat(row).hasSize(1);
      values.add(row.getFirst());
    }
    assertThat(values).containsExactlyElementsOf(engineValues(query, field));
    return values;
  }

  private BoltSystemProcedures.Served serve(final String query) {
    return BoltSystemProcedures.serveSchemaProcedure(db, BoltSystemProcedures.normalize(query));
  }

  private List<Object> engineValues(final String query, final String field) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet resultSet = db.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result result = resultSet.next();
        values.add(result.getProperty(field));
      }
    }
    return values;
  }

  /**
   * Stands in for {@code db.labels} in the registry and fails on execution, standing for any future registry
   * entry whose work is less trivial than iterating the schema.
   */
  private static class ExplodingLabels implements CypherProcedure {
    @Override
    public String getName() {
      return DbLabels.NAME;
    }

    @Override
    public int getMinArgs() {
      return 0;
    }

    @Override
    public int getMaxArgs() {
      return 0;
    }

    @Override
    public String getDescription() {
      return "Test double that fails on execution";
    }

    @Override
    public List<String> getYieldFields() {
      return List.of("label");
    }

    @Override
    public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
      throw new IllegalStateException("procedure blew up");
    }
  }
}
