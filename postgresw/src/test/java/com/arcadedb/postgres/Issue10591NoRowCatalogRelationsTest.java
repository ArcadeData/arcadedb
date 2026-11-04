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
import org.postgresql.jdbc.PgDatabaseMetaData;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10591: a catalog relation ArcadeDB can never hold a row in was refused with 0A000 (since #10371) instead of answered with
 * no rows, so psql's {@code \df} and {@code \du} and pgjdbc's {@code getImportedKeys()} and {@code getFunctions()} failed where
 * "there are none" is the honest answer. {@code pg_settings} holds a row per parameter in every PostgreSQL, so it is modelled
 * with the values {@code SHOW} answers, and a relation whose absence would be a lie is still refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10591NoRowCatalogRelationsTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10591NoRowCatalogRelationsTest";
  private static final String USER          = "root";

  /** psql's {@code \df}, verbatim from describe.c for a server of version 11 or later. */
  static final String PSQL_DF = """
      SELECT n.nspname as "Schema",
        p.proname as "Name",
        pg_catalog.pg_get_function_result(p.oid) as "Result data type",
        pg_catalog.pg_get_function_arguments(p.oid) as "Argument data types",
       CASE p.prokind
        WHEN 'a' THEN 'agg'
        WHEN 'w' THEN 'window'
        WHEN 'p' THEN 'proc'
        ELSE 'func'
       END as "Type"
      FROM pg_catalog.pg_proc p
           LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
      WHERE pg_catalog.pg_function_is_visible(p.oid)
            AND n.nspname <> 'pg_catalog'
            AND n.nspname <> 'information_schema'
      ORDER BY 1, 2, 4;""";

  /** psql's {@code \du}, verbatim from describe.c (psql 15), whose {@code memberof} sub-select reads pg_auth_members. */
  static final String PSQL_DU = """
      SELECT r.rolname, r.rolsuper, r.rolinherit,
        r.rolcreaterole, r.rolcreatedb, r.rolcanlogin,
        r.rolconnlimit, r.rolvaliduntil,
        ARRAY(SELECT b.rolname
              FROM pg_catalog.pg_auth_members m
              JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
              WHERE m.member = r.oid) as memberof
      , r.rolreplication
      , r.rolbypassrls
      FROM pg_catalog.pg_roles r
      WHERE r.rolname !~ '^pg_'
      ORDER BY 1;""";

  /**
   * The relations ArcadeDB has no equivalent of, whose honest answer is no row at all. pg_depend is not among them: it lists the
   * dependencies of the indexes (issue #10842) and of every table on its schema and its row type (issue #10961).
   */
  private static final List<String> NO_ROW_RELATIONS = List.of("pg_proc", "pg_trigger", "pg_auth_members", "pg_rewrite",
      "pg_sequence", "pg_shdescription", "pg_default_acl", "pg_foreign_table");

  /** Functions, schema names and pgjdbc's own prefixes that match the relation pattern but name no relation. */
  private static final Pattern NOT_A_RELATION = Pattern.compile("pg_catalog|pg_toast.*|pg_temp_|pg_get_.*|.*_is_visible|pg_get_keywords");

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> database.getSchema().createDocumentType("Person").createProperty("name", Type.STRING));
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void aRelationWithNoEquivalentAnswersNoRowAndItsCountZero() {
    for (final String relation : NO_ROW_RELATIONS) {
      assertThat(resolve("select * from pg_catalog." + relation).rows).as(relation).isEmpty();

      final PostgresCatalog.Answer count = resolve("select count(*) from pg_catalog." + relation);
      assertThat(count.rows).as(relation).hasSize(1);
      assertThat(((Number) count.rows.get(0).values().iterator().next()).longValue()).as(relation).isZero();
    }
  }

  @Test
  void aNoRowRelationAnswersItsColumnsByName() {
    final PostgresCatalog.Answer answer = resolve("select proname, prokind, pronamespace from pg_catalog.pg_proc");
    assertThat(answer.rows).isEmpty();
    assertThat(answer.columns.keySet()).containsExactly("proname", "prokind", "pronamespace");
  }

  @Test
  void psqlDescribeFunctionsAnswersNoFunction() {
    final PostgresCatalog.Answer answer = resolve(PSQL_DF);
    assertThat(answer.rows).isEmpty();
    assertThat(answer.columns.keySet()).containsExactly("Schema", "Name", "Result data type", "Argument data types", "Type");
  }

  @Test
  void psqlDescribeUsersListsTheUserWithNoMembership() {
    final PostgresCatalog.Answer answer = resolve(PSQL_DU);
    assertThat(answer.rows).hasSize(1);
    assertThat(answer.rows.get(0).get("rolname")).isEqualTo(USER);
    assertThat((List<?>) answer.rows.get(0).get("memberof")).isEmpty();
  }

  /** pgjdbc's {@code getFunctions()}, which reads the kind of a result with SQL's {@code substring(... from ... for ...)}. */
  @Test
  void pgjdbcGetFunctionsAnswersNoFunction() {
    final PostgresCatalog.Answer answer = resolve("SELECT current_database() AS \"FUNCTION_CAT\",  n.nspname AS \"FUNCTION_SCHEM\", "
        + "p.proname AS \"FUNCTION_NAME\",  d.description AS \"REMARKS\",  CASE    WHEN (format_type(p.prorettype, null) = 'unknown') "
        + "THEN 0   WHEN      (substring(pg_get_function_result(p.oid) from 0 for 6) = 'TABLE') OR      "
        + "(substring(pg_get_function_result(p.oid) from 0 for 6) = 'SETOF') THEN 2   ELSE 1 END  AS \"FUNCTION_TYPE\",  "
        + "p.proname || '_' || p.oid AS \"SPECIFIC_NAME\" FROM pg_catalog.pg_proc p INNER JOIN pg_catalog.pg_namespace n "
        + "ON p.pronamespace=n.oid LEFT JOIN pg_catalog.pg_description d ON p.oid=d.objoid WHERE true  AND p.prokind='f'  "
        + "AND p.proname LIKE '%'  ORDER BY \"FUNCTION_SCHEM\", \"FUNCTION_NAME\", p.oid::text");
    assertThat(answer.rows).isEmpty();
    assertThat(answer.columns.keySet()).containsExactly("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE",
        "SPECIFIC_NAME");
  }

  /** SQL's spelling of substring is the plain call PostgreSQL turns it into, including a start before the first character. */
  @Test
  void sqlSubstringSyntaxIsTheFunctionCall() {
    assertThat(resolve("select substring(relname from 0 for 3) as s from pg_class where relname = 'Person'").rows)
        .extracting(row -> row.get("s")).containsExactly("Pe");
    assertThat(resolve("select substring(relname from 3) as s from pg_class where relname = 'Person'").rows)
        .extracting(row -> row.get("s")).containsExactly("rson");
    assertThat(resolve("select substring(relname for 2) as s from pg_class where relname = 'Person'").rows)
        .extracting(row -> row.get("s")).containsExactly("Pe");
    assertThat(resolve("select substring(relname, 2, 3) as s from pg_class where relname = 'Person'").rows)
        .extracting(row -> row.get("s")).containsExactly("ers");
    // a text FROM is PostgreSQL's regular-expression extract, not a position: it is declined, never answered as one
    assertThat(PostgresCatalog.resolve("select substring(relname from 'P.r') as s from pg_class where relname = 'Person'", database,
        USER)).isSameAs(PostgresCatalog.DECLINED);
  }

  @Test
  void pgSettingsListsTheValuesShowAnswers() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.set("application_name", "app10591");

    final PostgresCatalog.Answer limit = PostgresCatalog.resolveForSession(
        "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'", database, USER, settings);
    assertThat(limit.rows).hasSize(1);
    assertThat(limit.rows.get(0).get("setting")).isEqualTo("32").isEqualTo(settings.show("max_index_keys"));

    final PostgresCatalog.Answer application = PostgresCatalog.resolveForSession(
        "select setting, context from pg_settings where name = 'application_name'", database, USER, settings);
    assertThat(application.rows).hasSize(1);
    assertThat(application.rows.get(0)).containsEntry("setting", "app10591").containsEntry("context", "user");

    // every row is what SHOW answers for the same name
    final PostgresCatalog.Answer all = PostgresCatalog.resolveForSession("select name, setting from pg_settings", database, USER,
        settings);
    assertThat(all.rows).hasSize(settings.showAll().size());
    for (final Map<String, Object> row : all.rows)
      assertThat(row.get("setting")).as("%s", row.get("name")).isEqualTo(settings.show((String) row.get("name")));
  }

  @Test
  void serverVersionNumReadsTheLeadingDigitsOfTheVersion() {
    assertThat(PostgresSessionSettings.serverVersionNum("12.0")).isEqualTo("120000");
    assertThat(PostgresSessionSettings.serverVersionNum("16.4 (ArcadeDB)")).isEqualTo("160004");
    assertThat(PostgresSessionSettings.serverVersionNum("17")).isEqualTo("170000");
    assertThat(PostgresSessionSettings.serverVersionNum("devel")).isEqualTo("0");
    assertThat(new PostgresSessionSettings().show("server_version_num")).isEqualTo(
        PostgresSessionSettings.serverVersionNum(PostgresNetworkExecutor.PG_SERVER_VERSION));
  }

  @Test
  void pgTablespaceListsTheTwoEveryPostgresqlHas() {
    final PostgresCatalog.Answer answer = resolve(
        "SELECT spcname AS \"Name\", pg_catalog.pg_get_userbyid(spcowner) AS \"Owner\", pg_catalog.pg_tablespace_location(oid) "
            + "AS \"Location\" FROM pg_catalog.pg_tablespace ORDER BY 1");
    assertThat(answer.rows).extracting(row -> row.get("Name")).containsExactly("pg_default", "pg_global");
    assertThat(answer.rows).extracting(row -> row.get("Location")).containsExactly("", "");
  }

  /** The refusal of #10371 stands for a relation that holds rows in PostgreSQL and is not modelled here. */
  @Test
  void aRelationWhoseAbsenceWouldBeALieIsStillDeclined() {
    for (final String relation : List.of("pg_stat_activity", "pg_extension", "pg_locks", "information_schema.routines"))
      assertThat(PostgresCatalog.resolve("select count(*) from " + relation, database, USER)).as(relation)
          .isSameAs(PostgresCatalog.DECLINED);
  }

  /**
   * The guard that stops this coming back for the driver every Java client goes through: every catalog relation the bundled
   * pgjdbc's {@code PgDatabaseMetaData} names is one this catalog models. A release of the driver that reads a new relation fails
   * here, naming it, rather than as a refused {@code DatabaseMetaData} call in someone's application.
   */
  @Test
  void everyRelationPgjdbcMetadataReadsIsModelled() throws Exception {
    final Set<String> read = new TreeSet<>();
    final Pattern name = Pattern.compile("(?<![A-Za-z0-9_])(?:pg_catalog\\.)?(pg_[a-z_]+)");
    try (final InputStream in = PgDatabaseMetaData.class.getResourceAsStream("PgDatabaseMetaData.class")) {
      assertThat(in).isNotNull();
      // the driver's SQL is in the constant pool of the class as plain text
      final Matcher matcher = name.matcher(new String(in.readAllBytes(), StandardCharsets.ISO_8859_1));
      while (matcher.find())
        if (!NOT_A_RELATION.matcher(matcher.group(1)).matches())
          read.add(matcher.group(1));
    }

    assertThat(read).as("relations named by the driver").contains("pg_class", "pg_proc", "pg_depend", "pg_settings");
    assertThat(PostgresCatalog.modelledRelations()).containsAll(read);
  }

  private PostgresCatalog.Answer resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as("query was not recognised as a catalog query: %s", query).isNotNull();
    assertThat(answer).as("query was declined: %s", query).isNotSameAs(PostgresCatalog.DECLINED);
    return answer;
  }
}
