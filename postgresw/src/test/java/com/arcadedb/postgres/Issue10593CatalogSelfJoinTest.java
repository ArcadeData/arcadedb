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
 * Issue #10593: a statement that reads one catalog relation through two aliases must bind each alias to its own rows.
 * <ul>
 * <li>A second alias on the right of a LEFT JOIN was answered on the one-row-per-entity path, where both aliases share one map, so a
 * condition that never holds within one row ({@code arr.oid = t.typarray}) read as "no match" and the client got NULL for a value
 * the comma and INNER JOIN spellings of the same question return.</li>
 * <li>An alias nothing reads was dropped from the FROM list, so {@code count(*)} over a self cross join counted one relation, and
 * the statement work budget never saw the product it is meant to stop.</li>
 * </ul>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue10593CatalogSelfJoinTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10593CatalogSelfJoinTest";
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

  // ---------------------------------------------------------------- the LEFT JOIN spelling of a self join

  @Test
  void leftJoinToTheArrayTypeAnswersTheArrayType() {
    final List<Map<String, Object>> rows = resolve("select t.typname, arr.typname as arrname from pg_catalog.pg_type t"
        + " left join pg_catalog.pg_type arr on arr.oid = t.typarray where t.typname = 'int4'").rows;
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("typname")).isEqualTo("int4");
    assertThat(rows.get(0).get("arrname")).isEqualTo("_int4");
  }

  @Test
  void leftJoinToTheElementTypeAnswersTheElementType() {
    final List<Map<String, Object>> rows = resolve("select t.typname, e.typname as elemname from pg_catalog.pg_type t"
        + " left join pg_catalog.pg_type e on e.oid = t.typelem where t.typname = '_int4'").rows;
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("typname")).isEqualTo("_int4");
    assertThat(rows.get(0).get("elemname")).isEqualTo("int4");
  }

  /** The differential the report asks for: the comma, INNER JOIN and LEFT JOIN spellings of one question agree where it matches. */
  @Test
  void theThreeSpellingsOfASelfJoinAgree() {
    final String comma = "select t.typname, arr.typname as arrname from pg_catalog.pg_type t, pg_catalog.pg_type arr"
        + " where t.typarray = arr.oid order by t.typname";
    final String inner = "select t.typname, arr.typname as arrname from pg_catalog.pg_type t"
        + " join pg_catalog.pg_type arr on arr.oid = t.typarray order by t.typname";
    final String left = "select t.typname, arr.typname as arrname from pg_catalog.pg_type t"
        + " left join pg_catalog.pg_type arr on arr.oid = t.typarray where arr.oid is not null order by t.typname";

    final List<Map<String, Object>> commaRows = resolve(comma).rows;
    assertThat(commaRows).isNotEmpty();
    assertThat(resolve(inner).rows).isEqualTo(commaRows);
    assertThat(resolve(left).rows).isEqualTo(commaRows);

    // the outer join adds exactly the types with no array type, each with a NULL partner
    final List<Map<String, Object>> all = resolve("select t.typname, arr.typname as arrname from pg_catalog.pg_type t"
        + " left join pg_catalog.pg_type arr on arr.oid = t.typarray").rows;
    final long types = count("select count(*) from pg_catalog.pg_type");
    assertThat(all).hasSize((int) types);
    assertThat(all).filteredOn(row -> row.get("arrname") == null).hasSize((int) (types - commaRows.size()));
  }

  /** The form the old guard was written for: psql's TOAST lookup, which matches nothing and must still read NULL. */
  @Test
  void leftJoinToASecondAliasThatMatchesNothingReadsNull() {
    final List<Map<String, Object>> rows = resolve("select c.relname, tc.relname as toast from pg_catalog.pg_class c"
        + " left join pg_catalog.pg_class tc on (c.reltoastrelid = tc.oid) where c.relname = 'Person'").rows;
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("relname")).isEqualTo("Person");
    assertThat(rows.get(0).get("toast")).isNull();
  }

  /** A second alias that matches its own row - the guard's live branch - is answered with that row rather than declined. */
  @Test
  void leftJoinToASecondAliasThatMatchesTheSameRow() {
    final List<Map<String, Object>> rows = resolve("select c.relname, c2.relname as same from pg_catalog.pg_class c"
        + " left join pg_catalog.pg_class c2 on c2.oid = c.oid where c.relname = 'City'").rows;
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get("same")).isEqualTo("City");
  }

  // ---------------------------------------------------------------- an unread FROM entry still multiplies

  @Test
  void anUnreadSecondAliasMultipliesTheRows() {
    final long classes = count("select count(*) from pg_catalog.pg_class");
    assertThat(classes).isGreaterThan(1);
    assertThat(count("select count(*) from pg_class a, pg_class b")).isEqualTo(classes * classes);
    // the two spellings of one relation are one relation
    assertThat(count("select count(*) from pg_catalog.pg_class a, pg_class b")).isEqualTo(classes * classes);
    assertThat(count("select count(*) from pg_class a, pg_class b where a.oid > 0 and b.oid > 0")).isEqualTo(classes * classes);
    assertThat(resolve("select a.relname as x, b.relname as y from pg_class a, pg_class b").rows).hasSize((int) (classes * classes));
    assertThat(count("select count(*) from (select a.relname as x, b.relname as y from pg_class a, pg_class b) t"))
        .isEqualTo(classes * classes);
  }

  @Test
  void anUnreadCrossJoinIsChargedToTheWorkBudget() {
    // enough columns that six of pg_attribute pair more rows than one statement may spend: 20^6 is 64 million
    database.transaction(() -> {
      final VertexType wide = database.getSchema().createVertexType("Wide");
      for (int i = 0; i < 16; i++)
        wide.createProperty("p" + i, Type.STRING);
    });
    final long attributes = count("select count(*) from pg_catalog.pg_attribute");
    assertThat(attributes).isGreaterThanOrEqualTo(20);

    final String unread = "select count(*) from pg_attribute a, pg_attribute b, pg_attribute c, pg_attribute d,"
        + " pg_attribute e, pg_attribute f";
    final String read = unread + " where a.attnum > 0 and b.attnum > 0 and c.attnum > 0 and d.attnum > 0 and e.attnum > 0"
        + " and f.attnum > 0";
    assertThatThrownBy(() -> PostgresCatalog.resolve(read, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    assertThatThrownBy(() -> PostgresCatalog.resolve(unread, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
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
