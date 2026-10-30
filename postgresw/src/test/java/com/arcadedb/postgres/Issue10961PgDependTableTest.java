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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10961: {@code pg_depend} lists the dependencies a stock PostgreSQL records for a user table beside the ones of its indexes
 * (issue #10842): the table on its schema ({@code deptype 'n'}) and the row type of the table on the table ({@code 'i'}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10961PgDependTableTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10961PgDependTableTest";
  private static final String USER          = "root";

  private static final int PG_CLASS     = 1259;
  private static final int PG_TYPE      = 1247;
  private static final int PG_NAMESPACE = 2615;

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final DocumentType account = schema.createDocumentType("Account");
      account.createProperty("note", Type.STRING);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Account", "note");
      schema.createDocumentType("Plain").createProperty("name", Type.STRING);
    });
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void aTableDependsOnItsSchema() {
    final int schemaOid = number(resolve("select oid from pg_namespace where nspname = '" + database.getName() + "'").rows.getFirst(), "oid");
    for (final String table : List.of("Account", "Plain")) {
      final List<Map<String, Object>> rows = resolve(
          "select classid, objsubid, refclassid, refobjid, refobjsubid, deptype from pg_depend where classid = " + PG_CLASS
              + " and refclassid = " + PG_NAMESPACE + " and objid = " + oidOfClass(table)).rows;
      assertThat(rows).as(table).hasSize(1);
      assertThat(rows.getFirst()).containsEntry("objsubid", 0).containsEntry("refobjid", schemaOid).containsEntry("refobjsubid", 0)
          .containsEntry("deptype", "n");
    }
  }

  @Test
  void theRowTypeOfATableDependsOnTheTable() {
    for (final String table : List.of("Account", "Plain")) {
      final int rowType = number(resolve("select reltype from pg_class where relname = '" + table + "'").rows.getFirst(), "reltype");
      final List<Map<String, Object>> rows = resolve(
          "select classid, objsubid, refclassid, refobjid, refobjsubid, deptype from pg_depend where objid = " + rowType).rows;
      assertThat(rows).as(table).hasSize(1);
      assertThat(rows.getFirst()).containsEntry("classid", PG_TYPE).containsEntry("objsubid", 0).containsEntry("refclassid", PG_CLASS)
          .containsEntry("refobjid", oidOfClass(table)).containsEntry("refobjsubid", 0).containsEntry("deptype", "i");
    }
  }

  /** The walk a tool makes from a schema: every table in it, and from each table its row type, joined to the catalogs by name. */
  @Test
  void walkingFromTheSchemaReachesTheTablesAndTheirRowTypes() {
    final List<Map<String, Object>> tables = resolve("""
        select c.relname from pg_depend d join pg_class c on c.oid = d.objid join pg_namespace n on n.oid = d.refobjid
        where d.classid = 'pg_class'::regclass and d.refclassid = 'pg_namespace'::regclass and d.deptype = 'n'
        and n.nspname = '%s' order by c.relname""".formatted(database.getName())).rows;
    assertThat(tables).extracting(row -> row.get("relname")).containsExactly("Account", "Plain");

    final List<Map<String, Object>> types = resolve("""
        select t.typname from pg_depend d join pg_type t on t.oid = d.objid join pg_class c on c.oid = d.refobjid
        where d.classid = 'pg_type'::regclass and d.refclassid = 'pg_class'::regclass and d.deptype = 'i'
        order by t.typname""").rows;
    assertThat(types).extracting(row -> row.get("typname")).containsExactly("Account", "Plain");
  }

  @Test
  void theCatalogsOfTheNewDependenciesAreRegclassNames() {
    final Map<String, Object> row = resolve("""
        select 'pg_namespace'::regclass as n, 'pg_catalog.pg_type'::regclass as t, 2615::regclass as nn, 1247::regclass as tn
        from pg_namespace limit 1""").rows.getFirst();
    assertThat(row).containsEntry("n", PG_NAMESPACE).containsEntry("t", PG_TYPE).containsEntry("nn", "pg_namespace")
        .containsEntry("tn", "pg_type");
  }

  private int oidOfClass(final String name) {
    final List<Map<String, Object>> rows = resolve("select oid from pg_class where relname = '" + name + "'").rows;
    assertThat(rows).as(name).hasSize(1);
    return number(rows.getFirst(), "oid");
  }

  private static int number(final Map<String, Object> row, final String column) {
    return ((Number) row.get(column)).intValue();
  }

  private PostgresCatalog.Answer resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as("query was not recognised as a catalog query: %s", query).isNotNull();
    assertThat(answer).as("query was declined: %s", query).isNotSameAs(PostgresCatalog.DECLINED);
    return answer;
  }
}
