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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10371: a catalog query over a relation the emulation does not model was answered with an empty result set, so an
 * ungrouped {@code count(*)} lost its one row (PostgreSQL always returns exactly one) and psql's index listings reported nothing
 * while {@code pg_class.relhasindex} said the table had indexes. The resolver says {@link PostgresCatalog#DECLINED}; the executor
 * now refuses it with {@code feature_not_supported} instead of sending an empty answer, and this catalog stops claiming indexes
 * it cannot show.
 * <p>
 * The contract is held for every relation at once: {@code select count(*) from <relation>} either answers its one row or is
 * declined - never zero rows - for each modelled relation and a list of unmodelled {@code pg_*} and {@code information_schema}
 * names, so a new relation, or a new reason to decline, cannot fall back into the empty answer by default.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10371UnmodelledRelationTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10371UnmodelledRelationTest";
  private static final String USER          = "root";

  /** Relations PostgreSQL has and this catalog does not model: some are empty in ArcadeDB's world, some are not. */
  private static final List<String> UNMODELLED = List.of("pg_catalog.pg_proc", "pg_proc", "pg_extension", "pg_trigger",
      "pg_stat_activity", "pg_settings", "pg_rewrite", "pg_tablespace", "information_schema.referential_constraints",
      "information_schema.table_privileges", "information_schema.routines", "information_schema.sequences",
      "information_schema.statistics");

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      schema.createDocumentType("Person").createProperty("name", Type.STRING);
      schema.getType("Person").createProperty("age", Type.INTEGER);
      schema.getType("Person").createProperty("tags", Type.LIST, "STRING");
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "name");
    });
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /** The guarantee of the issue, for every relation: one row, or a refusal. Never an answer with no row in it. */
  @Test
  void anUngroupedCountOverAnyRelationAnswersItsOneRowOrIsRefused() {
    final List<String> relations = new ArrayList<>(new TreeSet<>(PostgresCatalog.modelledRelations()));
    relations.addAll(UNMODELLED);
    assertThat(relations).as("relations checked").hasSizeGreaterThan(30);

    for (final String relation : relations) {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select count(*) from " + relation + " where 1 = 0", database,
          USER);

      assertThat(answer).as("an answer for %s, or the query reaches the SQL engine", relation).isNotNull();
      if (answer != PostgresCatalog.DECLINED)
        assertThat(answer.rows).as("count(*) over %s answers exactly one row", relation).hasSize(1);
    }
  }

  /** A relation PostgreSQL really has and this catalog does not model is declined, so the executor can refuse it. */
  @Test
  void anUnmodelledRelationIsDeclinedAndNeverAnsweredEmpty() {
    for (final String relation : UNMODELLED) {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select count(*) from " + relation, database, USER);
      assertThat(answer).as(relation).isSameAs(PostgresCatalog.DECLINED);
      assertThat(answer.rows).as("a refusal carries no rows to send: %s", relation).isNull();
    }
  }

  @Test
  void aRefusalNamesTheRelationItCouldNotModel() {
    assertThat(PostgresCatalog.refusalOf("select count(*) from pg_catalog.pg_proc")).isInstanceOf(
        PostgresCatalog.UnsupportedCatalogQuery.class).hasMessageContaining("\"pg_proc\"");
    assertThat(PostgresCatalog.refusalOf("select * from information_schema.routines where 1 = 1"))
        .hasMessageContaining("\"information_schema.routines\"");
    // a function is not a relation: the one named is the relation it is read from
    assertThat(PostgresCatalog.refusalOf("select pg_catalog.pg_get_functiondef(p.oid) from pg_catalog.pg_proc p"))
        .hasMessageContaining("\"pg_proc\"").hasMessageNotContaining("pg_get_functiondef");
    // a shape with no unmodelled relation in it still says it is refused, and why
    assertThat(PostgresCatalog.refusalOf("select relname from pg_class a, pg_class b, pg_class c where a.oid = b.oid"))
        .hasMessageContaining("shape");
    assertThat(PostgresCatalog.UnsupportedCatalogQuery.SQLSTATE).as("feature_not_supported").isEqualTo("0A000");
  }

  /** A column reference is not a relation: {@code pg_type.oid} must not be named as the relation that was refused. */
  @Test
  void aColumnReferenceIsNotNamedAsTheRefusedRelation() {
    assertThat(PostgresCatalog.refusalOf("select pg_type.oid from pg_catalog.pg_proc, pg_catalog.pg_type"))
        .hasMessageContaining("\"pg_proc\"").hasMessageNotContaining("pg_type.oid");
    assertThat(PostgresCatalog.refusalOf("select pg_type.oid from pg_catalog.pg_type, pg_class a, pg_class b, pg_class c"))
        .hasMessageContaining("shape");
  }

  /**
   * The lookups a driver sends to name a type it does not know have no exemption from the refusal (issue #10495): they are answered
   * from {@link PostgresType} (see {@code Issue10495TypeLookupTest}), and a type query this catalog cannot read is refused like any
   * other, whatever relations it names.
   */
  @Test
  void aTypeQueryThatCannotBeReadIsRefusedLikeAnyOtherCatalogQuery() {
    // another relation the catalog does not model
    assertThat(PostgresCatalog.resolve("select * from pg_type t, pg_proc p where p.prorettype = t.oid", database, USER))
        .isSameAs(PostgresCatalog.DECLINED);
    // a filter it cannot evaluate, which over the fixed set of types would otherwise select every one of them
    assertThat(PostgresCatalog.resolve("select t.oid from pg_type t where t.typname = lookup_something(t.oid)", database, USER))
        .isSameAs(PostgresCatalog.DECLINED);
    // a lookup of a table function the evaluator does not know
    assertThat(PostgresCatalog.resolve("select t.oid from pg_type t, generate_subscripts(current_schemas(true), 1) s", database,
        USER)).isSameAs(PostgresCatalog.DECLINED);
  }

  /** The control: a modelled relation with no matching row still answers PostgreSQL's one row holding 0. */
  @Test
  void aModelledRelationStillAnswersItsZero() {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select count(*) from pg_catalog.pg_class where relname = 'Nope'",
        database, USER);

    assertThat(answer.rows).hasSize(1);
    assertThat(((Number) answer.rows.get(0).values().iterator().next()).longValue()).isZero();
  }

  /**
   * {@code relhasindex} says what {@code pg_index} shows (issue #10473): psql's {@code \d} reads the flag first and lists the
   * indexes only when it is true. It was false for every type while pg_index was unmodelled.
   */
  @Test
  void relhasindexFollowsTheIndexesTheCatalogShows() {
    final PostgresCatalog.Answer classes = PostgresCatalog.resolve(
        "select relname, relhasindex from pg_catalog.pg_class where relname = 'Person'", database, USER);
    assertThat(classes.rows).hasSize(1);
    assertThat(classes.rows.get(0)).containsEntry("relhasindex", Boolean.TRUE);

    final PostgresCatalog.Answer tables = PostgresCatalog.resolve(
        "select tablename, hasindexes from pg_catalog.pg_tables where tablename = 'Person'", database, USER);
    assertThat(tables.rows).hasSize(1);
    assertThat(tables.rows.get(0)).containsEntry("hasindexes", Boolean.TRUE);

    assertThat(PostgresCatalog.resolve("select indexrelid from pg_catalog.pg_index", database, USER).rows).hasSize(1);
  }

  /** A type with no index says so in both places. */
  @Test
  void aTypeWithNoIndexSaysSo() {
    database.getSchema().createDocumentType("Plain");

    final PostgresCatalog.Answer classes = PostgresCatalog.resolve(
        "select relhasindex from pg_catalog.pg_class where relname = 'Plain'", database, USER);
    assertThat(classes.rows.get(0)).containsEntry("relhasindex", Boolean.FALSE);

    final PostgresCatalog.Answer tables = PostgresCatalog.resolve(
        "select hasindexes from pg_catalog.pg_tables where tablename = 'Plain'", database, USER);
    assertThat(tables.rows.get(0)).containsEntry("hasindexes", Boolean.FALSE);
  }
}
