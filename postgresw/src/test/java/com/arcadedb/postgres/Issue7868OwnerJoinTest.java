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
 * Regression test for issue #7868, the follow-up to #7224. Both halves of the same two-part pattern were still
 * open for {@code pg_roles} / {@code pg_user}:
 * <ol>
 * <li>they were populated only on a ROLES row, so a table, column, type or schema row joined to them read
 * {@code rolname} out of {@code Row.complete()}'s NULL fill - even though the very column being joined
 * ({@code relowner}, {@code nspowner}, {@code typowner}, {@code datdba}) carries the owner's OID and the catalog
 * answers {@code pg_get_userbyid()} with the name;</li>
 * <li>SCHEMAS still outranked ROLES, so a role query that merely names pg_namespace was answered as a schema
 * query - the exact defect of #7224, with pg_roles where pg_type was.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7868OwnerJoinTest {
  private static final String DATABASE_PATH = "./target/databases/Issue7868OwnerJoinTest";
  private static final String USER          = "root";

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

  /** The shape a GUI browser uses to list tables with their owner. */
  @Test
  void aTableRowCarriesTheRoleItsRelownerPointsAt() {
    final PostgresCatalog.Answer answer = resolve(
        "SELECT c.relname, c.relowner, r.oid, r.rolname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            + "JOIN pg_roles r ON r.oid = c.relowner");

    assertThat(names(answer.rows, "relname")).containsExactly("Article");
    assertThat(answer.rows).allSatisfy(row -> {
      assertThat(row.get("rolname")).as("the owner the catalog already knows, not a NULL fill").isEqualTo(USER);
      assertThat(row.get("oid")).as("the join key has to agree with the joined row").isEqualTo(row.get("relowner"));
    });
  }

  /** The pg_user spelling of the same question, which answered NULL the same way. */
  @Test
  void aTableRowCarriesThePgUserSpellingToo() {
    final PostgresCatalog.Answer answer = resolve(
        "SELECT c.relname, u.usename, u.usesysid, c.relowner FROM pg_class c JOIN pg_user u ON u.usesysid = c.relowner");

    assertThat(answer.rows).allSatisfy(row -> {
      assertThat(row.get("usename")).isEqualTo(USER);
      assertThat(row.get("usesysid")).isEqualTo(row.get("relowner"));
    });
  }

  @Test
  void aColumnRowATypeRowAndASchemaRowAllCarryTheOwnerAsWell() {
    assertThat(resolve("SELECT a.attname, r.rolname FROM pg_attribute a, pg_roles r").rows)
        .allSatisfy(row -> assertThat(row.get("rolname")).isEqualTo(USER));

    final PostgresCatalog.Answer types = resolve(
        "SELECT t.typname, t.typowner, r.oid, r.rolname FROM pg_type t JOIN pg_roles r ON r.oid = t.typowner");
    assertThat(types.rows).hasSize(PostgresTypeCatalog.types().size());
    assertThat(types.rows).allSatisfy(row -> {
      assertThat(row.get("rolname")).isEqualTo(USER);
      assertThat(row.get("oid")).isEqualTo(row.get("typowner"));
    });

    final PostgresCatalog.Answer schemas = resolve("SELECT n.nspname, n.nspowner, r.rolname FROM pg_namespace n, pg_roles r");
    assertThat(schemas.rows).allSatisfy(row -> {
      assertThat(row.get("nspname")).as("whichever family the ranking picks, both sides have values").isNotNull();
      assertThat(row.get("rolname")).isEqualTo(USER);
    });

    final PostgresCatalog.Answer databases = resolve(
        "SELECT d.datname, d.datdba, r.oid, r.rolname FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba");
    assertThat(databases.rows).allSatisfy(row -> {
      assertThat(row.get("rolname")).isEqualTo(USER);
      assertThat(row.get("oid")).isEqualTo(row.get("datdba"));
    });
  }

  /**
   * The ranking half. pg_namespace is the one relation this catalog models that can never be the subject of a
   * query naming anything else - it models exactly one schema - so every family that CAN be a subject now
   * outranks SCHEMAS, not only TYPES.
   */
  @Test
  void aRoleQueryThatNamesPgNamespaceIsStillAboutRoles() {
    final PostgresCatalog.Answer answer = resolve("SELECT r.rolname FROM pg_roles r JOIN pg_namespace n ON n.nspowner = r.oid");

    assertThat(answer.rows).hasSize(1);
    assertThat(answer.rows.get(0).get("rolname")).as("one nameless role, the #7224 outcome").isEqualTo(USER);
  }

  @Test
  void theOtherFormerlyUnrankedFamiliesNoLongerLoseToPgNamespace() {
    assertThat(names(resolve("SELECT d.datname FROM pg_database d, pg_namespace n").rows, "datname"))
        .containsExactly(database.getName());

    assertThat(names(resolve(
        "SELECT p.privilege_type FROM information_schema.usage_privileges p, pg_namespace n").rows, "privilege_type"))
        .containsExactly("USAGE");

    assertThat(names(resolve(
        "SELECT c.character_set_name FROM information_schema.character_sets c, pg_namespace n").rows, "character_set_name"))
        .containsExactly("UTF8");

    assertThat(names(resolve(
        "SELECT c.collation_name FROM information_schema.collations c, pg_namespace n").rows, "collation_name"))
        .containsExactly("default");
  }

  /** The FROM order must not decide it, for these families any more than it does for TYPES. */
  @Test
  void theFromOrderDoesNotChangeTheAnswer() {
    assertThat(resolve("SELECT r.rolname, n.nspname FROM pg_namespace n, pg_roles r").rows)
        .containsExactlyElementsOf(resolve("SELECT r.rolname, n.nspname FROM pg_roles r, pg_namespace n").rows);
  }

  /**
   * Found in review: the five families the ranking now ties with each other were still FROM-order dependent
   * PAIRWISE, because each built a row of its own. A ROLES row carried no {@code pg_database} columns, so
   * {@code SELECT r.rolname, d.datname FROM pg_roles r, pg_database d} read {@code datname} out of the null
   * fill, while the same query with the FROM order swapped answered it - the #7224 defect one level up, inside
   * the tied bucket. All six now answer the same row, so the tie cannot decide anything.
   */
  @Test
  void twoSingleRowFamiliesJoinedToEachOtherAnswerBothSidesWhicheverComesFirst() {
    final String[] relations = { "pg_namespace", "pg_roles", "pg_database", "information_schema.usage_privileges",
        "information_schema.character_sets", "information_schema.collations" };
    final String projection = "n.nspname, r.rolname, d.datname, p.privilege_type, c.character_set_name, l.collation_name";
    final String[] aliases = { "n", "r", "d", "p", "c", "l" };

    // Every ordering of the six that starts with a different one of them: whichever the ranking picks, the
    // projection has to be answerable in full.
    for (int first = 0; first < relations.length; first++) {
      final StringBuilder from = new StringBuilder();
      from.append(relations[first]).append(' ').append(aliases[first]);
      for (int i = 0; i < relations.length; i++)
        if (i != first)
          from.append(", ").append(relations[i]).append(' ').append(aliases[i]);

      final PostgresCatalog.Answer answer = resolve("SELECT " + projection + " FROM " + from);
      assertThat(answer.rows).as("one row per singleton, whichever relation is named first").hasSize(1);
      assertThat(answer.rows.get(0)).as("no column may fall through to Row.complete()'s null fill (first=%s)",
          relations[first]).doesNotContainValue(null);
    }
  }

  /** What must not move: the families that CONSTRAIN a row set still outrank the ones that only qualify it. */
  @Test
  void tablesColumnsAndTypesStillOutrankTheSingleRowFamilies() {
    assertThat(names(resolve("SELECT c.relname, r.rolname FROM pg_class c, pg_roles r").rows, "relname"))
        .containsExactly("Article");
    assertThat(names(resolve("SELECT a.attname, r.rolname FROM pg_attribute a, pg_roles r").rows, "attname"))
        .contains("id");
    assertThat(resolve("SELECT t.typname, r.rolname FROM pg_roles r, pg_type t").rows)
        .hasSize(PostgresTypeCatalog.types().size());
  }

  /**
   * Found in review: a type row is the one row whose subject lives in {@code pg_catalog} rather than in the
   * database's own schema, and only {@code pg_namespace} was moved there - {@code information_schema.schemata}
   * and {@code usage_privileges} still named the user's schema on the same row, so the relations it carries
   * contradicted each other. They move together now.
   */
  @Test
  void everyRelationOnATypeRowAgreesOnWhichSchemaTheTypeIsIn() {
    final PostgresCatalog.Answer answer = resolve(
        "SELECT t.typname, n.nspname, s.schema_name, s.catalog_name, p.object_schema, p.object_name, p.object_catalog "
            + "FROM pg_type t, pg_namespace n, information_schema.schemata s, information_schema.usage_privileges p");

    assertThat(answer.rows).hasSize(PostgresTypeCatalog.types().size());
    assertThat(answer.rows).allSatisfy(row -> {
      assertThat(row.get("nspname")).isEqualTo("pg_catalog");
      assertThat(row.get("schema_name")).as("schemata is the information_schema spelling of pg_namespace")
          .isEqualTo("pg_catalog");
      assertThat(row.get("object_schema")).isEqualTo("pg_catalog");
      assertThat(row.get("object_name")).isEqualTo("pg_catalog");
      // A catalog is a DATABASE, not a schema, so these stay put whichever schema the row describes.
      assertThat(row.get("catalog_name")).isEqualTo(database.getName());
      assertThat(row.get("object_catalog")).isEqualTo(database.getName());
    });

    // And a row that is NOT about a type still names the database's own schema everywhere.
    final PostgresCatalog.Answer schemas = resolve(
        "SELECT n.nspname, s.schema_name, p.object_schema FROM pg_namespace n, information_schema.schemata s, "
            + "information_schema.usage_privileges p");
    assertThat(schemas.rows).allSatisfy(row -> {
      assertThat(row.get("nspname")).isEqualTo(database.getName());
      assertThat(row.get("schema_name")).isEqualTo(database.getName());
      assertThat(row.get("object_schema")).isEqualTo(database.getName());
    });
  }

  /** And pg_namespace alone is still a question about schemas, one row. */
  @Test
  void pgNamespaceOnItsOwnIsStillASchemaQuery() {
    final PostgresCatalog.Answer answer = resolve("SELECT nspname FROM pg_catalog.pg_namespace");

    assertThat(answer.rows).hasSize(1);
    assertThat(answer.rows.get(0).get("nspname")).isEqualTo(database.getName());
  }

  private PostgresCatalog.Answer resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as("query was not recognised as a catalog query: %s", query).isNotNull();
    assertThat(answer).as("query was declined: %s", query).isNotSameAs(PostgresCatalog.DECLINED);
    return answer;
  }

  private static List<Object> names(final List<Map<String, Object>> rows, final String column) {
    return rows.stream().map(row -> row.get(column)).toList();
  }
}
