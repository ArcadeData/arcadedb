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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13387: {@code FROM information_schema.tables|columns|schemata} is served by the SQL engine itself, from the same schema calls
 * the PostgreSQL wire catalog reads. The tests create types and check the names, the types and the order of what the relations list.
 */
class InformationSchemaTest extends TestHelper {

  private List<Result> rows(final String sql) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.query("sql", sql)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }

  private List<String> column(final String sql, final String column) {
    final List<String> values = new ArrayList<>();
    for (final Result row : rows(sql))
      values.add(row.getProperty(column));
    return values;
  }

  private void createPersonTypes() {
    database.transaction(() -> {
      final DocumentType base = database.getSchema().createDocumentType("Base");
      base.createProperty("created", Type.DATETIME);
      base.createProperty("secret", Type.STRING).setHidden(true);

      final DocumentType person = database.getSchema().createDocumentType("Person");
      person.addSuperType(base);
      person.createProperty("name", Type.STRING).setMandatory(true);
      person.createProperty("age", Type.INTEGER).setDefaultValue("18");
      person.createProperty("height", Type.DOUBLE);
      person.createProperty("score", Type.FLOAT);
      person.createProperty("visits", Type.LONG).setNotNull(true);
      person.createProperty("level", Type.SHORT);
      person.createProperty("active", Type.BOOLEAN);
      person.createProperty("born", Type.DATE);
      person.createProperty("salary", Type.DECIMAL);
      person.createProperty("photo", Type.BINARY);
      person.createProperty("meta", Type.MAP);
      person.createProperty("tags", Type.LIST, "STRING");
      person.createProperty("lengths", Type.LIST, "INTEGER");
      person.createProperty("anything", Type.LIST);
    });
  }

  @Test
  void tablesListEveryTypeInNameOrderWithTheKindsOfTable() {
    database.getSchema().createDocumentType("Zeta");
    database.getSchema().createVertexType("Alpha");
    database.getSchema().createEdgeType("Mid");

    assertThat(column("SELECT table_name FROM information_schema.tables", "table_name")).containsExactly("Alpha", "Mid", "Zeta");

    final List<Result> rows = rows("SELECT * FROM information_schema.tables");
    assertThat(rows).hasSize(3);
    for (final Result row : rows) {
      assertThat(row.getPropertyNames()).containsExactly("table_catalog", "table_schema", "table_name", "table_type",
          "self_referencing_column_name", "reference_generation", "user_defined_type_catalog", "user_defined_type_schema",
          "user_defined_type_name", "is_insertable_into", "is_typed", "commit_action");
      assertThat(row.<String>getProperty("table_catalog")).isEqualTo(database.getName());
      assertThat(row.<String>getProperty("table_schema")).isEqualTo(database.getName());
      assertThat(row.<String>getProperty("table_type")).isEqualTo("BASE TABLE");
      assertThat(row.<String>getProperty("is_insertable_into")).isEqualTo("YES");
      assertThat(row.<String>getProperty("is_typed")).isEqualTo("NO");
      assertThat(row.<Object>getProperty("commit_action")).isNull();
    }
  }

  @Test
  void aTypeCreatedLaterAppearsAndADroppedOneLeaves() {
    database.getSchema().createDocumentType("First");
    assertThat(column("SELECT table_name FROM information_schema.tables", "table_name")).containsExactly("First");

    database.getSchema().createDocumentType("Second");
    assertThat(column("SELECT table_name FROM information_schema.tables", "table_name")).containsExactly("First", "Second");

    database.getSchema().dropType("First");
    assertThat(column("SELECT table_name FROM information_schema.tables", "table_name")).containsExactly("Second");
  }

  @Test
  void columnsListThePropertiesOfATableInNameOrderWithTheirTypes() {
    createPersonTypes();

    final List<Result> rows = rows("SELECT * FROM information_schema.columns WHERE table_name = 'Person'");

    // the own properties and the inherited one, by name, the hidden one left out
    assertThat(column("SELECT column_name FROM information_schema.columns WHERE table_name = 'Person' ORDER BY ordinal_position",
        "column_name")).containsExactly("active", "age", "anything", "born", "created", "height", "lengths", "level", "meta",
        "name", "photo", "salary", "score", "tags", "visits");
    assertThat(rows).hasSize(15);
    assertThat(rows.get(0).getPropertyNames()).containsExactly("table_catalog", "table_schema", "table_name", "column_name",
        "ordinal_position", "column_default", "is_nullable", "data_type", "character_maximum_length", "character_octet_length",
        "numeric_precision", "numeric_precision_radix", "numeric_scale", "datetime_precision", "udt_catalog", "udt_schema",
        "udt_name", "is_identity", "identity_generation", "is_generated", "generation_expression", "is_updatable");

    // the ordinal positions count from 1 without a gap
    final List<Integer> ordinals = new ArrayList<>();
    for (final Result row : rows("SELECT ordinal_position FROM information_schema.columns WHERE table_name = 'Person' "
        + "ORDER BY ordinal_position"))
      ordinals.add(row.getProperty("ordinal_position"));
    assertThat(ordinals).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

    assertColumn("active", "boolean", "bool", "YES");
    assertColumn("age", "integer", "int4", "YES");
    assertColumn("anything", "ARRAY", "_text", "YES");
    assertColumn("born", "date", "date", "YES");
    assertColumn("created", "timestamp without time zone", "timestamp", "YES");
    assertColumn("height", "double precision", "float8", "YES");
    assertColumn("lengths", "ARRAY", "_int4", "YES");
    assertColumn("level", "smallint", "int2", "YES");
    assertColumn("meta", "json", "json", "YES");
    assertColumn("name", "character varying", "varchar", "NO");
    assertColumn("photo", "bytea", "bytea", "YES");
    assertColumn("salary", "numeric", "numeric", "YES");
    assertColumn("score", "real", "float4", "YES");
    assertColumn("tags", "ARRAY", "_text", "YES");
    assertColumn("visits", "bigint", "int8", "NO");
  }

  private void assertColumn(final String name, final String dataType, final String udtName, final String nullable) {
    final List<Result> found = rows("SELECT * FROM information_schema.columns WHERE table_name = 'Person' AND column_name = '"
        + name + "'");
    assertThat(found).as(name).hasSize(1);
    final Result row = found.get(0);
    assertThat(row.<String>getProperty("table_catalog")).as(name).isEqualTo(database.getName());
    assertThat(row.<String>getProperty("table_schema")).as(name).isEqualTo(database.getName());
    assertThat(row.<String>getProperty("data_type")).as(name).isEqualTo(dataType);
    assertThat(row.<String>getProperty("udt_name")).as(name).isEqualTo(udtName);
    assertThat(row.<String>getProperty("udt_schema")).as(name).isEqualTo("pg_catalog");
    assertThat(row.<String>getProperty("is_nullable")).as(name).isEqualTo(nullable);
  }

  @Test
  void columnsReportTheFiguresOfTheirType() {
    createPersonTypes();

    final Result age = rows("SELECT * FROM information_schema.columns WHERE table_name = 'Person' AND column_name = 'age'").get(0);
    assertThat(age.<String>getProperty("column_default")).isEqualTo("18");
    assertThat(age.<Integer>getProperty("numeric_precision")).isEqualTo(32);
    assertThat(age.<Integer>getProperty("numeric_precision_radix")).isEqualTo(2);
    assertThat(age.<Integer>getProperty("numeric_scale")).isEqualTo(0);

    final Result name = rows("SELECT * FROM information_schema.columns WHERE table_name = 'Person' AND column_name = 'name'")
        .get(0);
    assertThat(name.<Object>getProperty("column_default")).isNull();
    assertThat(name.<Integer>getProperty("character_octet_length")).isEqualTo(1 << 30);
    assertThat(name.<Object>getProperty("numeric_precision")).isNull();

    final Result salary = rows("SELECT * FROM information_schema.columns WHERE table_name = 'Person' AND column_name = 'salary'")
        .get(0);
    assertThat(salary.<Integer>getProperty("numeric_precision_radix")).isEqualTo(10);
    assertThat(salary.<Object>getProperty("numeric_precision")).isNull();

    final Result born = rows("SELECT * FROM information_schema.columns WHERE table_name = 'Person' AND column_name = 'born'").get(0);
    assertThat(born.<Integer>getProperty("datetime_precision")).isEqualTo(0);

    final Result created = rows("SELECT * FROM information_schema.columns WHERE table_name = 'Person' AND column_name = 'created'")
        .get(0);
    assertThat(created.<Integer>getProperty("datetime_precision")).isEqualTo(6);

    // an array carries none of them
    final Result tags = rows("SELECT * FROM information_schema.columns WHERE table_name = 'Person' AND column_name = 'tags'")
        .get(0);
    assertThat(tags.<Object>getProperty("numeric_precision")).isNull();
    assertThat(tags.<Object>getProperty("character_octet_length")).isNull();
  }

  @Test
  void schemataListTheDatabaseSchema() {
    final List<Result> rows = rows("SELECT * FROM information_schema.schemata");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getPropertyNames()).containsExactly("catalog_name", "schema_name", "schema_owner",
        "default_character_set_catalog", "default_character_set_schema", "default_character_set_name", "sql_path");
    assertThat(column("SELECT schema_name FROM information_schema.schemata", "schema_name")).containsExactly("pg_catalog",
        database.getName());
    assertThat(column("SELECT catalog_name FROM information_schema.schemata", "catalog_name")).containsOnly(database.getName());
  }

  @Test
  void aTypeThatHoldsAMaterializedViewIsNotATable() {
    database.getSchema().createDocumentType("P");
    database.command("sql", "CREATE MATERIALIZED VIEW MV AS SELECT FROM P REFRESH INCREMENTAL");

    assertThat(database.getSchema().existsType("MV")).isTrue();
    assertThat(column("SELECT table_name FROM information_schema.tables", "table_name")).containsExactly("P");
    assertThat(rows("SELECT FROM information_schema.columns WHERE table_name = 'MV'")).isEmpty();
  }

  @Test
  void theRowsGoThroughTheUsualOperators() {
    database.getSchema().createDocumentType("A");
    database.getSchema().createDocumentType("B");
    database.getSchema().createDocumentType("C");
    database.getSchema().createDocumentType("D");

    // WHERE, an alias, ORDER BY, SKIP and LIMIT, which page the listing a batch at a time
    assertThat(column("SELECT table_name FROM information_schema.tables t WHERE t.table_name <> 'B' ORDER BY table_name DESC",
        "table_name")).containsExactly("D", "C", "A");
    assertThat(column("SELECT table_name FROM information_schema.tables SKIP 1 LIMIT 2", "table_name")).containsExactly("B", "C");
    assertThat(column("SELECT table_name FROM information_schema.tables LIMIT 2", "table_name")).containsExactly("A", "B");

    final List<Result> count = rows("SELECT count(*) AS n FROM information_schema.tables");
    assertThat(count).hasSize(1);
    assertThat(count.get(0).<Long>getProperty("n")).isEqualTo(4L);

    // an unquoted name is not case sensitive
    assertThat(column("SELECT table_name FROM INFORMATION_SCHEMA.TABLES", "table_name")).containsExactly("A", "B", "C", "D");
  }

  @Test
  void aQueryThroughTheCommandPathAnswersToo() {
    database.getSchema().createDocumentType("A");
    try (final ResultSet resultSet = database.command("sql", "SELECT table_name FROM information_schema.tables")) {
      assertThat(resultSet.hasNext()).isTrue();
      assertThat(resultSet.next().<String>getProperty("table_name")).isEqualTo("A");
      assertThat(resultSet.hasNext()).isFalse();
    }
  }

  @Test
  void explainNamesTheStep() {
    for (final String relation : new String[] { "tables", "columns", "schemata" }) {
      try (final ResultSet resultSet = database.query("sql", "EXPLAIN SELECT FROM information_schema." + relation)) {
        assertThat(resultSet.hasNext()).isTrue();
        final String plan = resultSet.next().getProperty("executionPlanAsString");
        assertThat(plan).as(relation).contains("FETCH INFORMATION_SCHEMA " + relation);
      }
    }
  }

  @Test
  void aRelationTheEngineDoesNotServeIsRefused() {
    assertThatThrownBy(() -> rows("SELECT FROM information_schema.routines")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("routines");
  }

  @Test
  void aTypeNamedInformationSchemaKeepsItsMeaning() {
    database.getSchema().createDocumentType("information_schema");
    database.transaction(() -> database.newDocument("information_schema").set("tables", "mine").save());

    assertThat(column("SELECT tables FROM information_schema", "tables")).containsExactly("mine");
    try (final ResultSet resultSet = database.query("sql", "EXPLAIN SELECT FROM information_schema.tables")) {
      assertThat(resultSet.hasNext()).isTrue();
      final String plan = resultSet.next().getProperty("executionPlanAsString");
      assertThat(plan).doesNotContain("FETCH INFORMATION_SCHEMA");
    }
  }
}
