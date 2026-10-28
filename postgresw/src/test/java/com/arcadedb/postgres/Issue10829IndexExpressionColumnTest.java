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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10829: a key of an index over a property that is {@code HIDDEN} is no column of its table in this catalog, so it was
 * numbered 0 in {@code indkey} - PostgreSQL's "expression column" - and given no {@code pg_attribute} row, while {@code relnatts}
 * of the index still counted it.
 * <p>
 * Such a key is now what PostgreSQL makes of an expression column: a {@code pg_attribute} row of the index named {@code expr}
 * ({@code expr1}, ... for the next ones), the expression in {@code indexprs}, and - for a unique index - no {@code pg_constraint},
 * since a unique constraint is over columns.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10829IndexExpressionColumnTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10829IndexExpressionColumnTest";
  private static final String USER          = "root";
  private static final String UNIQUE_INDEX  = "Account[code,secret]";
  private static final String HIDDEN_INDEX  = "Account[secret]";

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
      account.createProperty("code", Type.STRING);
      account.createProperty("secret", Type.INTEGER);
      account.createProperty("note", Type.STRING);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Account", "code", "secret");
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Account", "secret");
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Account", "note");
    });
    database.command("sql", "ALTER PROPERTY Account.secret HIDDEN true");
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /** The invariant PostgreSQL keeps for an index as for a table: relnatts is the number of its pg_attribute rows. */
  @Test
  void relnattsOfEveryIndexCountsItsAttributeRows() {
    final PostgresCatalog.Answer indexes = resolve("select oid, relname, relnatts from pg_class where relkind = 'i' order by relname");
    assertThat(column(indexes, "relname")).containsExactly(UNIQUE_INDEX, "Account[note]", HIDDEN_INDEX);

    for (final Map<String, Object> index : indexes.rows) {
      final long attributes = count("select count(*) from pg_attribute where attrelid = " + index.get("oid") + " and attnum > 0");
      assertThat(attributes).as("the attribute rows of %s", index.get("relname")).isEqualTo(((Number) index.get("relnatts")).longValue());
    }
  }

  /** The key over the hidden property is an expression column: attnum 0 in indkey, named expr, typed from the property. */
  @Test
  void theHiddenKeyIsAnExpressionColumnOfTheIndex() {
    final PostgresCatalog.Answer attributes = resolve("""
        select a.attname, a.attnum, a.atttypid from pg_attribute a join pg_class c on c.oid = a.attrelid
        where c.relname = 'Account[code,secret]' order by a.attnum""");
    assertThat(column(attributes, "attname")).containsExactly("code", "expr");
    assertThat(column(attributes, "attnum")).containsExactly(1, 2);
    assertThat(column(attributes, "atttypid")).containsExactly(1043, 23);

    final PostgresCatalog.Answer index = resolve("""
        select i.indkey, i.indnatts, pg_catalog.pg_get_expr(i.indexprs, i.indrelid) as exprs, i.indisunique
        from pg_index i join pg_class c on c.oid = i.indexrelid where c.relname = 'Account[code,secret]'""");
    // code is the table's first column; the hidden property is none of its columns
    assertThat(column(index, "indkey").get(0).toString()).isEqualTo("1 0");
    assertThat(column(index, "indnatts")).containsExactly(2);
    assertThat(column(index, "exprs")).containsExactly("secret");
    assertThat(column(index, "indisunique")).containsExactly(true);

    // an index of only plain columns has no expression
    assertThat(column(resolve("""
        select i.indexprs from pg_index i join pg_class c on c.oid = i.indexrelid where c.relname = 'Account[note]'"""), "indexprs"))
        .containsExactly((Object) null);
  }

  /** The table's own columns still leave the hidden property out, and the index's attribute does not name it. */
  @Test
  void theHiddenPropertyStaysOutOfTheColumnsOfTheTable() {
    assertThat(column(resolve("""
        select a.attname from pg_attribute a join pg_class c on c.oid = a.attrelid where c.relname = 'Account' order by 1"""),
        "attname")).containsExactly("code", "note");
    assertThat(column(resolve("""
        select a.attname from pg_attribute a join pg_class c on c.oid = a.attrelid where c.relname = 'Account[secret]'"""),
        "attname")).containsExactly("expr");
  }

  /**
   * A unique index with an expression key is not a unique constraint, as in PostgreSQL, where a constraint is over columns: no
   * {@code pg_constraint} row, no {@code information_schema} constraint and no key column naming what is no column of the table.
   */
  @Test
  void aUniqueIndexWithAnExpressionKeyIsNoConstraint() {
    assertThat(resolve("select conname from pg_constraint").rows).isEmpty();
    assertThat(resolve("select constraint_name from information_schema.table_constraints").rows).isEmpty();
    assertThat(resolve("select column_name from information_schema.key_column_usage").rows).isEmpty();
  }

  /** The column names PostgreSQL's ChooseIndexColumnNames gives two expression columns of one index: expr, then expr1. */
  @Test
  void twoExpressionColumnsOfOneIndexAreNumbered() {
    database.transaction(() -> {
      final DocumentType pair = database.getSchema().createDocumentType("Pair");
      pair.createProperty("x", Type.STRING);
      pair.createProperty("y", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Pair", "x", "y");
    });
    database.command("sql", "ALTER PROPERTY Pair.x HIDDEN true");
    database.command("sql", "ALTER PROPERTY Pair.y HIDDEN true");

    assertThat(column(resolve("""
        select a.attname from pg_attribute a join pg_class c on c.oid = a.attrelid where c.relname = 'Pair[x,y]' order by a.attnum"""),
        "attname")).containsExactly("expr", "expr1");
    assertThat(column(resolve("""
        select pg_catalog.pg_get_expr(i.indexprs, i.indrelid) as exprs from pg_index i join pg_class c on c.oid = i.indexrelid
        where c.relname = 'Pair[x,y]'"""), "exprs")).containsExactly("x, y");
  }

  // ---------------------------------------------------------------- helpers

  private long count(final String query) {
    final PostgresCatalog.Answer answer = resolve(query);
    assertThat(answer.rows).as("one row: %s", query).hasSize(1);
    return ((Number) answer.rows.get(0).get("count")).longValue();
  }

  private PostgresCatalog.Answer resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as("answered: %s", query).isNotNull();
    assertThat(answer.rows).as("answered rather than declined: %s", query).isNotNull();
    return answer;
  }

  private static List<Object> column(final PostgresCatalog.Answer answer, final String name) {
    final List<Object> values = new ArrayList<>();
    for (final Map<String, Object> row : answer.rows)
      values.add(row.get(name));
    return values;
  }
}
