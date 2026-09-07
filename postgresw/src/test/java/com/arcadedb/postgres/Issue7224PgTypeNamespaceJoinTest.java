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
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7224: {@code pg_type} joined to {@code pg_namespace} was answered as a question about
 * SCHEMAS, because SCHEMAS outranked TYPES in the family ranking. The single fabricated schema row then had every
 * {@code pg_type} column filled with NULL by {@code Row.complete()}, so the projection validated and the client got
 * one nameless type instead of the type list - silently, with no error.
 * <p>
 * The join is a qualification, not a constraint: {@code pg_namespace} is named purely to attach a schema to each
 * type, exactly as PostgreSQL's own {@code DatabaseMetaData.getTypeInfo()} writes it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7224PgTypeNamespaceJoinTest {
  private static final String DATABASE_PATH = "./target/databases/Issue7224PgTypeNamespaceJoinTest";
  private static final String USER          = "root";

  /** {@code DatabaseMetaData.getTypeInfo()} exactly as pgjdbc writes it. */
  private static final String JDBC_GET_TYPE_INFO =
      "SELECT t.typname,t.oid FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
          + "WHERE n.nspname  != 'pg_toast' AND (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c "
          + "WHERE c.oid = t.typrelid))";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> database.getSchema().createDocumentType("Article").createProperty("id", Type.INTEGER));
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void getTypeInfoAnswersTheTypeListRatherThanOneNullSchemaRow() {
    final PostgresCatalog.Answer answer = resolve(JDBC_GET_TYPE_INFO);

    assertThat(answer.rows).as("one fabricated all-NULL row instead of the type list").hasSizeGreaterThan(1);
    assertThat(answer.rows).allSatisfy(row -> {
      assertThat(row.get("typname")).isNotNull();
      assertThat(row.get("oid")).isNotNull();
    });

    // The same set the dedicated pg_type recogniser answers with, so the two surfaces cannot disagree.
    assertThat(names(answer.rows, "typname")).containsExactlyInAnyOrderElementsOf(
        PostgresTypeCatalog.types().stream().map(t -> PostgresTypeCatalog.columnValue(t, "typname")).toList());
  }

  @Test
  void aPlainJoinOfTypeAndNamespaceIsAboutTypes() {
    final PostgresCatalog.Answer answer = resolve(
        "SELECT t.typname, n.nspname, n.oid, t.typnamespace FROM pg_type t JOIN pg_namespace n ON (t.typnamespace = n.oid)");

    assertThat(answer.rows).hasSize(PostgresTypeCatalog.types().size());
    assertThat(names(answer.rows, "typname")).doesNotContainNull();
    // The schema a built-in type lives in is pg_catalog, which is also what pg_type.typnamespace already says -
    // so the column the query joins ON and the row it joins TO have to agree, or the join it wrote is a lie.
    assertThat(names(answer.rows, "nspname")).containsOnly("pg_catalog");
    assertThat(answer.rows).allSatisfy(row -> assertThat(row.get("oid")).isEqualTo(row.get("typnamespace")));
  }

  /** The FROM order must not decide it: a client is free to name pg_namespace first. */
  @Test
  void theFromOrderDoesNotChangeTheAnswer() {
    final PostgresCatalog.Answer answer = resolve("SELECT t.typname FROM pg_namespace n, pg_type t");

    assertThat(answer.rows).hasSize(PostgresTypeCatalog.types().size());
    assertThat(names(answer.rows, "typname")).doesNotContainNull();
  }

  /**
   * The other half of the ranking must not move: {@code pg_type} joined to {@code pg_class} or {@code pg_attribute}
   * CONSTRAINS the row set - one row per table or per column - and answering it with one row per type would change
   * what every JDBC {@code getColumns()} means.
   */
  @Test
  void pgClassAndPgAttributeStillOutrankPgType() {
    assertThat(names(resolve("SELECT c.relname, t.typname FROM pg_class c, pg_type t").rows, "relname")).containsExactly(
        "Article");

    final PostgresCatalog.Answer columns = resolve("SELECT a.attname, t.typname FROM pg_attribute a, pg_type t");
    assertThat(names(columns.rows, "attname")).contains("id");
  }

  /**
   * The families that used to TIE with TYPES at rank 0 - ROLES, DATABASES, PRIVILEGES, CHARACTER_SETS, COLLATIONS -
   * now lose to it, so the FROM order no longer settles them. That is a side effect of the ranking this issue
   * changed, and it is the same reading: pg_roles joined to pg_type reads an owner for each type, so it qualifies.
   * The old javadoc admitted the tie was accidental rather than considered, which is precisely why it needs pinning
   * now that it is neither.
   */
  @Test
  void typesWinsOverTheFamiliesItUsedToTieWith() {
    final PostgresCatalog.Answer typeFirst = resolve("SELECT t.typname, r.rolname FROM pg_type t, pg_roles r");
    final PostgresCatalog.Answer rolesFirst = resolve("SELECT t.typname, r.rolname FROM pg_roles r, pg_type t");

    assertThat(typeFirst.rows).hasSize(PostgresTypeCatalog.types().size());
    assertThat(names(typeFirst.rows, "typname")).doesNotContainNull();
    // The two orders have to agree on the ROWS, not merely on how many there are: a ranking regression could keep
    // the count and the non-null names while answering about a different relation.
    assertThat(rolesFirst.rows).containsExactlyElementsOf(typeFirst.rows);

    // And pg_roles on its own is still a question about roles: one row, not one per type.
    assertThat(resolve("SELECT rolname FROM pg_roles").rows).hasSize(1);
  }

  /** And pg_namespace alone is still a question about schemas. */
  @Test
  void pgNamespaceOnItsOwnIsStillASchemaQuery() {
    final PostgresCatalog.Answer answer = resolve("SELECT nspname FROM pg_catalog.pg_namespace");

    assertThat(answer.rows).hasSize(1);
    assertThat(answer.rows.get(0).get("nspname")).isNotNull();
  }

  /** A three-way join keeps naming the most specific subject: pg_class still wins over both. */
  @Test
  void tablesStillWinWhenNamespaceAndTypeAreBothJoinedToPgClass() {
    final PostgresCatalog.Answer answer = resolve("SELECT c.relname FROM pg_class c, pg_namespace n, pg_type t");

    assertThat(names(answer.rows, "relname")).containsExactly("Article");
  }

  /**
   * A scalar sub-select in the predicate is an operand this catalog cannot compute, not a reason to abandon the
   * whole predicate: the readable half still constrains, and three-valued logic settles the rest. What it must
   * NOT do is weaken the pg_type strictness - a predicate whose value is still UNKNOWN once the subquery has
   * answered UNKNOWN is declined exactly as an unparseable one was.
   */
  @Test
  void aScalarSubqueryIsAnUnknownOperandRatherThanAnUnreadablePredicate() {
    // typrelid is 0 for every type this protocol produces, so `= 0 OR <unknown>` is TRUE for all of them.
    assertThat(resolve("SELECT typname FROM pg_type t WHERE t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_class c "
        + "WHERE c.oid = t.typrelid)").rows).hasSize(PostgresTypeCatalog.types().size());

    // With nothing readable left to decide it, the answer is still a decline rather than "every type".
    assertThat(PostgresCatalog.resolve("SELECT typname FROM pg_type t WHERE (SELECT c.relkind = 'c' FROM pg_class c "
        + "WHERE c.oid = t.typrelid)", database, USER)).isSameAs(PostgresCatalog.DECLINED);
  }

  private PostgresCatalog.Answer resolve(final String query, final Object... parameters) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER, parameters);
    assertThat(answer).as("query was not recognised as a catalog query: %s", query).isNotNull();
    assertThat(answer).as("query was declined: %s", query).isNotSameAs(PostgresCatalog.DECLINED);
    return answer;
  }

  private static List<Object> names(final List<Map<String, Object>> rows, final String column) {
    return rows.stream().map(row -> row.get(column)).toList();
  }
}
