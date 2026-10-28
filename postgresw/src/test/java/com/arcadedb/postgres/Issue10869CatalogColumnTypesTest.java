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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10869: the columns of a catalog answer were typed by sampling the rows that came back, so an answer with no row carried no
 * type at all - a pg_type lookup of a type the server does not have announced ZERO fields for a three-column projection, and every
 * other relation announced every column as varchar where the same projection with a matching filter announced int4 or bool.
 * <p>
 * The census holds every modelled relation to one contract: its columns are typed by the relation, the values its rows carry are of
 * those types, and a filter that matches nothing announces exactly what a filter that matches announces.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10869CatalogColumnTypesTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10869CatalogColumnTypesTest";
  private static final String USER          = "root";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    // a schema that gives every relation rows: properties of every kind with defaults and constraints, indexes of every
    // family, an inheritance, a materialized view and a continuous aggregate
    database.command("sqlscript", """
        CREATE DOCUMENT TYPE Probe;
        CREATE PROPERTY Probe.s STRING (mandatory true, notnull true, default 'x');
        CREATE PROPERTY Probe.i INTEGER (default 3);
        CREATE PROPERTY Probe.l LONG;
        CREATE PROPERTY Probe.d DOUBLE;
        CREATE PROPERTY Probe.f FLOAT;
        CREATE PROPERTY Probe.b BOOLEAN;
        CREATE PROPERTY Probe.dt DATETIME;
        CREATE PROPERTY Probe.da DATE;
        CREATE PROPERTY Probe.dec DECIMAL;
        CREATE PROPERTY Probe.bin BINARY;
        CREATE PROPERTY Probe.lst LIST OF STRING;
        CREATE PROPERTY Probe.m MAP;
        CREATE PROPERTY Probe.sh SHORT;
        CREATE PROPERTY Probe.bt BYTE;
        CREATE PROPERTY Probe.txt STRING;
        CREATE INDEX ON Probe (s) UNIQUE;
        CREATE INDEX ON Probe (i, l) NOTUNIQUE;
        CREATE INDEX ON Probe (txt) FULL_TEXT;
        CREATE VERTEX TYPE Parent;
        CREATE PROPERTY Parent.name STRING;
        CREATE VERTEX TYPE Child EXTENDS Parent;
        CREATE EDGE TYPE Knows;
        BEGIN;
        INSERT INTO Probe SET s = 'a', i = 1, l = 2, d = 1.5, f = 2.5, b = true, sh = 1, bt = 2, dec = 1.25;
        COMMIT;
        CREATE MATERIALIZED VIEW ProbeView AS SELECT s, i FROM Probe;
        CREATE TIMESERIES TYPE Metrics TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE);
        """);
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE MetricsHourly AS SELECT host, avg(v) AS av, ts.timeBucket('1h', ts) AS hour FROM Metrics GROUP BY host, hour");
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void everyRelationAnnouncesTheSameColumnsWhetherItsFilterMatchesOrNot() {
    final TreeSet<String> checked = new TreeSet<>();
    for (final String relation : new TreeSet<>(PostgresCatalog.modelledRelations())) {
      final PostgresCatalog.Answer all = PostgresCatalog.resolve("SELECT * FROM " + relation, database, USER);
      final PostgresCatalog.Answer none = PostgresCatalog.resolve("SELECT * FROM " + relation + " WHERE 1 = 0", database, USER);
      if (all == null || all == PostgresCatalog.DECLINED) {
        // a relation that is only ever read joined to another one (pg_description) is not answered on its own either way
        assertThat(none == null || none == PostgresCatalog.DECLINED).as(relation).isTrue();
        continue;
      }
      assertThat(none).as(relation).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
      assertThat(none.rows).as(relation).isEmpty();

      assertThat(none.columns).as("the columns of %s with no row", relation).containsExactlyEntriesOf(all.columns);
      for (final Map.Entry<String, PostgresType> column : all.columns.entrySet())
        assertThat(column.getValue()).as("%s.%s", relation, column.getKey())
            .isEqualTo(PostgresCatalog.declaredType(relation, column.getKey()));
      checked.add(relation);
    }
    assertThat(checked).contains("pg_class", "pg_attribute", "pg_type", "information_schema.columns", "pg_index", "pg_constraint");
  }

  /**
   * The drift guard on the declarations: a value a relation carries is of the type the relation announces its column with, for every
   * row of every relation of a schema that populates all of them. A row builder that starts producing a different type, or a
   * column that starts carrying values without a declaration, fails here rather than in a client that typed its schema from the OIDs.
   */
  @Test
  void everyValueIsOfTheTypeItsColumnIsDeclaredWith() {
    int typedValues = 0;
    for (final String relation : new TreeSet<>(PostgresCatalog.modelledRelations())) {
      final PostgresCatalog.Answer all = PostgresCatalog.resolve("SELECT * FROM " + relation, database, USER);
      if (all == null || all == PostgresCatalog.DECLINED)
        continue;
      for (final Map<String, Object> row : all.rows)
        for (final Map.Entry<String, Object> value : row.entrySet()) {
          if (value.getValue() == null)
            continue;
          assertThat(typeOf(value.getValue())).as("%s.%s = %s", relation, value.getKey(), value.getValue())
              .isEqualTo(PostgresCatalog.declaredType(relation, value.getKey()));
          ++typedValues;
        }
    }
    assertThat(typedValues).isGreaterThan(500);
  }

  @Test
  void theShapesOfTheIssueAnnounceTheirColumnsWithNoRow() {
    assertSameColumns("SELECT oid, relnatts, relhasindex, relkind FROM pg_class WHERE relname = 'Probe'",
        "SELECT oid, relnatts, relhasindex, relkind FROM pg_class WHERE relname = 'NoSuchRelation'");
    assertSameColumns("SELECT attrelid, attname, attnum, atttypid, attnotnull FROM pg_attribute WHERE attname = 's'",
        "SELECT attrelid, attname, attnum, atttypid, attnotnull FROM pg_attribute WHERE attrelid = 0");
    assertSameColumns("SELECT table_name, ordinal_position, numeric_precision FROM information_schema.columns WHERE table_name='Probe'",
        "SELECT table_name, ordinal_position, numeric_precision FROM information_schema.columns WHERE table_name='NoSuch'");
    // aliases and qualifiers keep the column's type
    assertSameColumns("SELECT c.oid AS id, c.relhasindex FROM pg_class c WHERE c.relname = 'Probe'",
        "SELECT c.oid AS id, c.relhasindex FROM pg_class c WHERE c.relname = 'NoSuch'");

    final PostgresCatalog.Answer typed = PostgresCatalog.resolve(
        "SELECT oid, relnatts, relhasindex, relkind FROM pg_class WHERE relname = 'NoSuchRelation'", database, USER);
    assertThat(typed.columns).containsExactly(Map.entry("oid", PostgresType.INTEGER), Map.entry("relnatts", PostgresType.INTEGER),
        Map.entry("relhasindex", PostgresType.BOOLEAN), Map.entry("relkind", PostgresType.VARCHAR));
  }

  @Test
  void aSetOperationWithAnEmptyArmKeepsTheTypesOfItsColumns() {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(
        "SELECT oid, relhasindex FROM pg_class WHERE relname = 'NoSuch' UNION ALL SELECT oid, relhasindex FROM pg_class WHERE relname = 'Nope'",
        database, USER);
    assertThat(answer).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    assertThat(answer.rows).isEmpty();
    assertThat(answer.columns).containsExactly(Map.entry("oid", PostgresType.INTEGER), Map.entry("relhasindex", PostgresType.BOOLEAN));
  }

  @Test
  void aTypeLookupThatFindsNoTypeStillAnnouncesTheProjectedColumns() {
    final PostgresCatalog.Answer byName = PostgresTypeCatalog.resolveAnswer("SELECT oid, typname, typlen FROM pg_type WHERE typname = 'int4'");
    final PostgresCatalog.Answer noName = PostgresTypeCatalog.resolveAnswer(
        "SELECT oid, typname, typlen FROM pg_type WHERE typname = 'nosuchtype'");
    final PostgresCatalog.Answer byOid = PostgresTypeCatalog.resolveAnswer("SELECT oid, typname, typlen FROM pg_type WHERE oid = 23");
    final PostgresCatalog.Answer noOid = PostgresTypeCatalog.resolveAnswer("SELECT oid, typname, typlen FROM pg_type WHERE oid = 999999");

    assertThat(byName.rows).hasSize(1);
    assertThat(noName.rows).isEmpty();
    assertThat(noOid.rows).isEmpty();
    for (final PostgresCatalog.Answer answer : new PostgresCatalog.Answer[] { byName, noName, byOid, noOid })
      assertThat(answer.columns).containsExactly(Map.entry("oid", PostgresType.INTEGER), Map.entry("typname", PostgresType.VARCHAR),
          Map.entry("typlen", PostgresType.INTEGER));

    // the element self-join of a scalar selects nothing, under the columns it projected
    final PostgresCatalog.Answer scalarElement = PostgresTypeCatalog.resolveAnswer(
        "SELECT e.typdelim, e.typname FROM pg_type t, pg_type e WHERE t.oid = 23 AND t.typelem = e.oid");
    assertThat(scalarElement.rows).isEmpty();
    assertThat(scalarElement.columns.keySet()).containsExactly("typdelim", "typname");

    // the recogniser and the relational catalog announce pg_type's columns alike
    final PostgresCatalog.Answer relational = PostgresCatalog.resolve("SELECT * FROM pg_type", database, USER);
    assertThat(PostgresTypeCatalog.resolveAnswer("SELECT * FROM pg_type WHERE oid = 999999").columns).allSatisfy(
        (name, type) -> assertThat(type).as(name).isEqualTo(relational.columns.get(name)));
  }

  private void assertSameColumns(final String matching, final String empty) {
    final PostgresCatalog.Answer some = PostgresCatalog.resolve(matching, database, USER);
    final PostgresCatalog.Answer none = PostgresCatalog.resolve(empty, database, USER);
    assertThat(some.rows).as(matching).isNotEmpty();
    assertThat(none.rows).as(empty).isEmpty();
    assertThat(none.columns).as(empty).containsExactlyEntriesOf(some.columns);
  }

  /** The type a catalog value is announced as when it is sampled: what the declarations must agree with. */
  private static PostgresType typeOf(final Object value) {
    if (value instanceof PostgresCatalogIndexes.Vector)
      return PostgresType.VARCHAR;
    final PostgresType sampled = PostgresType.getTypeForValue(value);
    return sampled.isArrayType() || sampled.isNativeScalarType() ? sampled : PostgresType.VARCHAR;
  }
}
