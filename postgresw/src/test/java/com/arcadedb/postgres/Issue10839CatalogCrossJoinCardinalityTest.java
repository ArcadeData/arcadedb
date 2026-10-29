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
package com.arcadedb.postgres;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10839: a FROM entry of a different relation that no condition joins to the others must multiply the rows, as it does in
 * PostgreSQL. The one-row-per-entity path answered such a statement with one row per entity of the most specific family, every
 * other entry reading the map of that same row, so {@code count(*)} over {@code pg_class, pg_namespace} counted the classes, and
 * the work budget never saw the product.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue10839CatalogCrossJoinCardinalityTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10839CatalogCrossJoinCardinalityTest";
  private static final String USER          = "root";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final VertexType person = schema.createVertexType("Person");
      person.createProperty("name", Type.STRING);
      person.createProperty("age", Type.INTEGER);
      schema.createVertexType("City").createProperty("name", Type.STRING);
      schema.createDocumentType("Note").createProperty("text", Type.STRING);
    });
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  // ---------------------------------------------------------------- a statement that reads no entry: only its cardinality

  @Test
  void countOverACommaJoinOfTwoRelationsCountsTheProduct() {
    final long classes = count("select count(*) from pg_class");
    final long namespaces = count("select count(*) from pg_namespace");
    final long attributes = count("select count(*) from pg_attribute");
    final long types = count("select count(*) from pg_type");
    // the fixture must make a missing product visible: one relation of more than one row on each side
    assertThat(classes).isGreaterThan(1);
    assertThat(namespaces).isGreaterThan(1);

    assertThat(count("select count(*) from pg_class a, pg_namespace b")).isEqualTo(classes * namespaces);
    assertThat(count("select count(*) from pg_catalog.pg_namespace b, pg_catalog.pg_class a")).isEqualTo(classes * namespaces);
    assertThat(count("select count(*) from pg_class c, pg_attribute a")).isEqualTo(classes * attributes);
    assertThat(count("select count(*) from pg_namespace n, pg_type t")).isEqualTo(namespaces * types);
    assertThat(count("select count(*) from pg_class c, pg_namespace n, pg_attribute a")).isEqualTo(
        classes * namespaces * attributes);
  }

  @Test
  void countOverACrossJoinCountsTheProduct() {
    final long classes = count("select count(*) from pg_class");
    final long namespaces = count("select count(*) from pg_namespace");

    assertThat(count("select count(*) from pg_class c cross join pg_namespace n")).isEqualTo(classes * namespaces);
    // an inner join whose condition reads no column is a cross join too
    assertThat(count("select count(*) from pg_class c join pg_namespace n on true")).isEqualTo(classes * namespaces);
  }

  @Test
  void aProjectionOfLiteralsHasOneRowPerPairOfRows() {
    final long classes = count("select count(*) from pg_class");
    final long namespaces = count("select count(*) from pg_namespace");

    assertThat(resolve("select 1 as one from pg_class c, pg_namespace n").rows).hasSize((int) (classes * namespaces));
  }

  @Test
  void aDerivedTableOverACrossJoinHasTheProductOfRows() {
    final long classes = count("select count(*) from pg_class");
    final long namespaces = count("select count(*) from pg_namespace");

    assertThat(count("select count(*) from (select 1 as one from pg_class c, pg_namespace n) t")).isEqualTo(classes * namespaces);
  }

  // ---------------------------------------------------------------- a statement whose entries are joined: answered as it was

  @Test
  void aJoinOnTheNaturalKeyStillHasOneRowPerClass() {
    final long classes = count("select count(*) from pg_class");

    assertThat(count("select count(*) from pg_class c join pg_namespace n on c.relnamespace = n.oid")).isEqualTo(classes);
    assertThat(count("select count(*) from pg_class c, pg_namespace n where c.relnamespace = n.oid")).isEqualTo(classes);
    // an unqualified column is read too: relnamespace is pg_class's, and the condition joins the two entries
    assertThat(count("select count(*) from pg_class c, pg_namespace n where relnamespace = n.oid")).isEqualTo(classes);
  }

  // ---------------------------------------------------------------- the work budget sees the product of an unjoined entry

  @Test
  void aReadCrossJoinOfDifferentRelationsIsChargedToTheWorkBudget() {
    database.transaction(() -> {
      final VertexType wide = database.getSchema().createVertexType("Wide");
      for (int i = 0; i < 60; i++)
        wide.createProperty("p" + i, Type.STRING);
    });

    final long product = count("select count(*) from pg_attribute") * count("select count(*) from information_schema.columns")
        * count("select count(*) from pg_type") * count("select count(*) from pg_class") * count("select count(*) from pg_tables")
        * count("select count(*) from pg_namespace") * count("select count(*) from information_schema.schemata");
    // what PostgreSQL would answer is more rows than one statement may spend
    assertThat(product).isGreaterThan(5_000_000L);

    final String unjoined = "select a.attname, col.column_name, t.typname, c.relname, tb.tablename, n.nspname, s.schema_name"
        + " from pg_attribute a, information_schema.columns col, pg_type t, pg_class c, pg_tables tb, pg_namespace n,"
        + " information_schema.schemata s";
    assertThatThrownBy(() -> PostgresCatalog.resolve(unjoined, database, USER)).isInstanceOf(
        PostgresCatalog.WorkLimitExceeded.class);

    // and a filter on one entry only does not join the others to it
    assertThatThrownBy(() -> PostgresCatalog.resolve(unjoined + " where c.relname = 'Wide'", database, USER)).isInstanceOf(
        PostgresCatalog.WorkLimitExceeded.class);
  }

  @Test
  void aJoinedStatementOverTheSameRelationsIsNotCharged() {
    database.transaction(() -> {
      final VertexType wide = database.getSchema().createVertexType("Wide");
      for (int i = 0; i < 60; i++)
        wide.createProperty("p" + i, Type.STRING);
    });

    // the JDBC driver's getColumns() shape: every entry joined on its key, one row per column
    final List<Map<String, Object>> rows = resolve("select n.nspname, c.relname, a.attname from pg_namespace n"
        + " join pg_class c on c.relnamespace = n.oid join pg_attribute a on a.attrelid = c.oid"
        + " where c.relname = 'Wide' and a.attnum > 0").rows;
    assertThat(rows).hasSize(60);
  }

  // ---------------------------------------------------------------- helpers

  private long count(final String query) {
    final List<Map<String, Object>> rows = resolve(query).rows;
    assertThat(rows).as("one row: %s", query).hasSize(1);
    return ((Number) rows.get(0).values().iterator().next()).longValue();
  }

  private PostgresCatalog.Answer resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as("answered: %s", query).isNotNull();
    assertThat(answer.rows).as("answered rather than declined: %s", query).isNotNull();
    return answer;
  }
}
