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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10960: the catalog relations psql's {@code \dy}, {@code \dew}, {@code \des}, {@code \deu}, {@code \dRs}, {@code \drds},
 * {@code \lo_list} and {@code \dconfig+} read hold no row in a stock PostgreSQL either, and describe objects ArcadeDB has no
 * equivalent of, so they are answered with no rows instead of the 0A000 of an unmodelled relation (#10842).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10960NoRowPsqlRelationsTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10960NoRowPsqlRelationsTest";
  private static final String USER          = "root";

  private static final List<String> RELATIONS = List.of("pg_event_trigger", "pg_foreign_data_wrapper", "pg_foreign_server",
      "pg_user_mappings", "pg_subscription", "pg_db_role_setting", "pg_largeobject_metadata", "pg_parameter_acl");

  /** psql's {@code \dy}, from describe.c (psql 17). */
  private static final String PSQL_DY = """
      SELECT evtname as "Name", evtevent as "Event", pg_catalog.pg_get_userbyid(e.evtowner) as "Owner",
       case evtenabled when 'O' then 'enabled'  when 'R' then 'replica'  when 'A' then 'always'  when 'D' then 'disabled' end as "Enabled",
       e.evtfoid::pg_catalog.regproc as "Function", pg_catalog.obj_description(e.oid, 'pg_event_trigger') as "Description"
      FROM pg_catalog.pg_event_trigger e
      ORDER BY 1""";

  /** psql's {@code \dew}. */
  private static final String PSQL_DEW = """
      SELECT fdw.fdwname AS "Name",
        pg_catalog.pg_get_userbyid(fdw.fdwowner) AS "Owner",
        fdw.fdwhandler::pg_catalog.regproc AS "Handler",
        fdw.fdwvalidator::pg_catalog.regproc AS "Validator"
      FROM pg_catalog.pg_foreign_data_wrapper fdw
      ORDER BY 1""";

  /** psql's {@code \des}. */
  private static final String PSQL_DES = """
      SELECT s.srvname AS "Name",
        pg_catalog.pg_get_userbyid(s.srvowner) AS "Owner",
        f.fdwname AS "Foreign-data wrapper"
      FROM pg_catalog.pg_foreign_server s
           JOIN pg_catalog.pg_foreign_data_wrapper f ON f.oid=s.srvfdw
      ORDER BY 1""";

  /** psql's {@code \deu}. */
  private static final String PSQL_DEU = """
      SELECT um.srvname AS "Server", um.usename AS "User name"
      FROM pg_catalog.pg_user_mappings um
      ORDER BY 1, 2""";

  /** psql's {@code \dRs}. */
  private static final String PSQL_DRS = """
      SELECT subname AS "Name", pg_catalog.pg_get_userbyid(subowner) AS "Owner", subenabled AS "Enabled",
        subpublications AS "Publication"
      FROM pg_catalog.pg_subscription
      WHERE subdbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database())
      ORDER BY 1""";

  /** psql's {@code \drds}. */
  private static final String PSQL_DRDS = """
      SELECT rolname AS "Role", datname AS "Database",
      pg_catalog.array_to_string(setconfig, E'\\n') AS "Settings"
      FROM pg_catalog.pg_db_role_setting s
      LEFT JOIN pg_catalog.pg_database d ON d.oid = setdatabase
      LEFT JOIN pg_catalog.pg_roles r ON r.oid = setrole
      ORDER BY 1, 2""";

  /** psql's {@code \lo_list}. */
  private static final String PSQL_LO_LIST = """
      SELECT oid as "ID",
        pg_catalog.pg_get_userbyid(lomowner) as "Owner",
        pg_catalog.obj_description(oid, 'pg_largeobject') as "Description"
      FROM pg_catalog.pg_largeobject_metadata
      ORDER BY oid""";

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
  void eachRelationAnswersNoRowAndItsCountZero() {
    for (final String relation : RELATIONS) {
      assertThat(PostgresCatalog.resolve("select * from pg_catalog." + relation, database, USER)).as(relation)
          .isNotSameAs(PostgresCatalog.DECLINED);
      assertThat(resolve("select * from pg_catalog." + relation).rows).as(relation).isEmpty();
      assertThat(resolve("select * from " + relation).rows).as(relation + " unqualified").isEmpty();

      final PostgresCatalog.Answer count = resolve("select count(*) from pg_catalog." + relation);
      assertThat(count.rows).as(relation).hasSize(1);
      assertThat(((Number) count.rows.get(0).values().iterator().next()).longValue()).as(relation).isZero();
    }
  }

  @Test
  void aColumnIsAnsweredByName() {
    final PostgresCatalog.Answer answer = resolve("select srvname, srvowner, srvfdw from pg_catalog.pg_foreign_server");
    assertThat(answer.rows).isEmpty();
    assertThat(answer.columns.keySet()).containsExactly("srvname", "srvowner", "srvfdw");
  }

  @Test
  void psqlCommandsAnswerNoRow() {
    assertThat(resolve(PSQL_DY).columns.keySet()).containsExactly("Name", "Event", "Owner", "Enabled", "Function", "Description");
    assertThat(resolve(PSQL_DY).rows).isEmpty();
    assertThat(resolve(PSQL_DEW).columns.keySet()).containsExactly("Name", "Owner", "Handler", "Validator");
    assertThat(resolve(PSQL_DEW).rows).isEmpty();
    assertThat(resolve(PSQL_DES).columns.keySet()).containsExactly("Name", "Owner", "Foreign-data wrapper");
    assertThat(resolve(PSQL_DES).rows).isEmpty();
    assertThat(resolve(PSQL_DEU).columns.keySet()).containsExactly("Server", "User name");
    assertThat(resolve(PSQL_DEU).rows).isEmpty();
    assertThat(resolve(PSQL_DRS).columns.keySet()).containsExactly("Name", "Owner", "Enabled", "Publication");
    assertThat(resolve(PSQL_DRS).rows).isEmpty();
    assertThat(resolve(PSQL_DRDS).columns.keySet()).containsExactly("Role", "Database", "Settings");
    assertThat(resolve(PSQL_DRDS).rows).isEmpty();
    assertThat(resolve(PSQL_LO_LIST).columns.keySet()).containsExactly("ID", "Owner", "Description");
    assertThat(resolve(PSQL_LO_LIST).rows).isEmpty();
  }

  /** A relation PostgreSQL holds rows in that this catalog does not model is still refused: the change is not "answer none". */
  @Test
  void aRelationThatHoldsRowsInEveryPostgresIsStillRefused() {
    for (final String relation : List.of("pg_extension", "pg_language", "pg_operator"))
      assertThat(PostgresCatalog.resolve("select * from pg_catalog." + relation, database, USER)).as(relation)
          .isSameAs(PostgresCatalog.DECLINED);
  }

  private PostgresCatalog.Answer resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as(query).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    return answer;
  }
}
