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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10495: the seven lookups pgjdbc's {@code TypeInfoCache} sends when it meets a type it does not know were declined by the
 * catalog evaluator and kept on an empty-answer shim ({@code PostgresCatalog.isTypeResolutionProbe}), so a driver was told "no such
 * type" for a type this server has. {@code pg_type} is the closed set of types {@link PostgresType} knows, so each lookup has a real
 * answer for a name or an OID it holds, and only the others are empty.
 * <p>
 * The evaluator now reads what the lookups are written with - a table function in FROM ({@code generate_series}, {@code unnest}),
 * {@code USING (column)}, {@code = ANY(array)}, a derived table joined to other entries, and a second alias of {@code pg_type} - and
 * each is pinned on its own as well, so the answer does not depend on how one driver version spells its lookup.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10495TypeLookupTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10495TypeLookupTest";
  private static final String USER          = "root";

  /** pgjdbc: the OID of a type named without a schema. */
  private static final String OID_OF_NAME =
      "SELECT pg_type.oid, typname FROM pg_catalog.pg_type LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r from pg_namespace as ns "
          + "join (select s.r, (current_schemas(false))[s.r] as nspname from generate_series(1, array_upper(current_schemas(false), 1)) "
          + "as s(r)) as r using (nspname)) as sp ON sp.nspoid = typnamespace WHERE typname = $1 ORDER BY sp.r, pg_type.oid DESC LIMIT 1;";

  /** pgjdbc: the array type of an element type. */
  private static final String ARRAY_OF_TYPE =
      "SELECT t.typarray, arr.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid "
          + "JOIN pg_catalog.pg_type arr ON arr.oid = t.typarray WHERE t.typname = $1 AND (n.nspname = $2 OR $3 AND n.nspname = ANY "
          + "(current_schemas(true))) ORDER BY t.oid DESC LIMIT 1";

  /** pgjdbc: the array type that has a given element, found through a scalar sub-select. */
  private static final String ARRAY_OF_ELEMENT =
      "SELECT t.oid, t.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.typelem = "
          + "(SELECT oid FROM pg_catalog.pg_type WHERE typname = $1) AND substring(t.typname, 1, 1) = '_' AND t.typlen = -1 ORDER BY "
          + "t.typelem DESC LIMIT 1";

  /** pgjdbc: the name of a type, and whether its schema is on the search path. */
  private static final String NAME_OF_OID =
      "SELECT n.nspname = ANY(current_schemas(true)), n.nspname, t.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n "
          + "ON t.typnamespace = n.oid WHERE t.oid = $1";

  /** pgjdbc: the delimiter of an array type. */
  private static final String DELIMITER_OF_ARRAY =
      "SELECT e.typdelim FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = $1 and t.typelem = e.oid";

  /** pgjdbc: the element of an array type. */
  private static final String ELEMENT_OF_ARRAY =
      "SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_type e "
          + "ON t.typelem = e.oid JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = $1";

  /** A driver that loads every type up front, composites, ranges and enums included. */
  private static final String EVERY_TYPE =
      "SELECT ns.nspname, t.oid, t.typname FROM pg_type AS t JOIN pg_namespace AS ns ON (ns.oid = typnamespace) "
          + "LEFT JOIN pg_range r ON r.rngtypid = t.oid LEFT JOIN pg_enum e ON e.enumtypid = t.oid";

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

  private PostgresCatalog.Answer resolve(final String query, final Object... parameters) {
    return PostgresCatalog.resolve(query, database, USER, parameters);
  }

  /** The values of each row, in the order the statement projected them. */
  private List<List<Object>> rowsOf(final String query, final Object... parameters) {
    final PostgresCatalog.Answer answer = resolve(query, parameters);
    assertThat(answer).as(query).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
    assertThat(answer.rows).as("rows of %s", query).isNotNull();
    final List<List<Object>> rows = new ArrayList<>(answer.rows.size());
    for (final Map<String, Object> row : answer.rows)
      rows.add(new ArrayList<>(row.values()));
    return rows;
  }

  @Test
  void theOidOfATypeNameIsTheOidOfTheTypeThatHasIt() {
    assertThat(rowsOf(OID_OF_NAME, "int4")).containsExactly(List.of(23, "int4"));
    assertThat(rowsOf(OID_OF_NAME, "_int4")).containsExactly(List.of(1007, "_int4"));
    assertThat(rowsOf(OID_OF_NAME, "text")).containsExactly(List.of(25, "text"));
    assertThat(rowsOf(OID_OF_NAME, "bool")).containsExactly(List.of(16, "bool"));
  }

  @Test
  void aTypeNameNoTypeHasIsAnEmptyAnswerAndNotARefusal() {
    assertThat(rowsOf(OID_OF_NAME, "hstore")).isEmpty();
    assertThat(rowsOf(OID_OF_NAME, "")).isEmpty();
  }

  @Test
  void theArrayTypeOfAnElementTypeIsFoundOnTheSearchPathOrInANamedSchema() {
    // pgjdbc passes the schema it was given and whether to look on the search path when it was given none
    assertThat(rowsOf(ARRAY_OF_TYPE, "int4", "", true)).containsExactly(List.of(1007, "_int4"));
    assertThat(rowsOf(ARRAY_OF_TYPE, "int4", "pg_catalog", false)).containsExactly(List.of(1007, "_int4"));
    assertThat(rowsOf(ARRAY_OF_TYPE, "int4", "other", false)).isEmpty();
    assertThat(rowsOf(ARRAY_OF_TYPE, "int4", "", false)).isEmpty();
    assertThat(rowsOf(ARRAY_OF_TYPE, "hstore", "", true)).isEmpty();
    // a type with no array type of its own has nothing to answer
    assertThat(rowsOf(ARRAY_OF_TYPE, "_int4", "", true)).isEmpty();
  }

  @Test
  void theArrayTypeThatHasAnElementIsFoundThroughAScalarSubSelect() {
    assertThat(rowsOf(ARRAY_OF_ELEMENT, "int4")).containsExactly(List.of(1007, "_int4"));
    assertThat(rowsOf(ARRAY_OF_ELEMENT, "text")).containsExactly(List.of(1009, "_text"));
    assertThat(rowsOf(ARRAY_OF_ELEMENT, "hstore")).isEmpty();
  }

  @Test
  void theNameOfAnOidSaysWhetherItsSchemaIsOnTheSearchPath() {
    assertThat(rowsOf(NAME_OF_OID, 23)).containsExactly(List.of(true, "pg_catalog", "int4"));
    assertThat(rowsOf(NAME_OF_OID, 1007)).containsExactly(List.of(true, "pg_catalog", "_int4"));
    assertThat(rowsOf(NAME_OF_OID, 999_999)).isEmpty();
  }

  @Test
  void theDelimiterOfAnArrayIsTheOneOfItsElement() {
    assertThat(rowsOf(DELIMITER_OF_ARRAY, 1007)).containsExactly(List.of(","));
    // a type that is no array has no element, so the join selects nothing
    assertThat(rowsOf(DELIMITER_OF_ARRAY, 23)).isEmpty();
    assertThat(rowsOf(DELIMITER_OF_ARRAY, 999_999)).isEmpty();
  }

  @Test
  void theElementOfAnArrayIsNamedWithItsSchema() {
    assertThat(rowsOf(ELEMENT_OF_ARRAY, 1007)).containsExactly(List.of(23, true, "pg_catalog", "int4"));
    assertThat(rowsOf(ELEMENT_OF_ARRAY, 1009)).containsExactly(List.of(25, true, "pg_catalog", "text"));
    assertThat(rowsOf(ELEMENT_OF_ARRAY, 23)).isEmpty();
  }

  @Test
  void everyTypeIsListedWithItsSchemaAndNoRangeOrEnum() {
    final List<List<Object>> rows = rowsOf(EVERY_TYPE);
    assertThat(rows).hasSize(PostgresTypeCatalog.types().size());
    for (final PostgresType type : PostgresTypeCatalog.types())
      assertThat(rows).contains(List.of("pg_catalog", type.code, type.typeName));
  }

  @Test
  void anOidAnArrayAndItsElementAgreeWithPostgresType() {
    for (final PostgresType type : PostgresTypeCatalog.types()) {
      assertThat(rowsOf(OID_OF_NAME, type.typeName)).as(type.typeName).containsExactly(List.of(type.code, type.typeName));
      assertThat(rowsOf(NAME_OF_OID, type.code)).as("oid %d", type.code).containsExactly(List.of(true, "pg_catalog", type.typeName));
      if (type.isArrayType())
        assertThat(rowsOf(ELEMENT_OF_ARRAY, type.code)).as("element of %s", type.typeName)
            .hasSize(1).first().satisfies(row -> assertThat(row.get(0)).isEqualTo(type.elementCode));
    }
  }

  // ---------------------------------------------------------------- what the evaluator reads, each on its own

  @Test
  void aTableFunctionInFromIsARelationOfItsOwn() {
    assertThat(rowsOf("select s.r from pg_database d, generate_series(1, 4) as s(r)")).containsExactly(List.of(1L), List.of(2L),
        List.of(3L), List.of(4L));
    assertThat(rowsOf("select s.r from pg_database d, generate_series(1, 9, 4) as s(r)")).containsExactly(List.of(1L), List.of(5L),
        List.of(9L));
    assertThat(rowsOf("select s.r from pg_database d, generate_series(3, 1, -1) as s(r)")).containsExactly(List.of(3L), List.of(2L),
        List.of(1L));
    assertThat(rowsOf("select s.r from pg_database d, generate_series(2, 1) as s(r)")).isEmpty();
    assertThat(rowsOf("select u.x from pg_database d, unnest(current_schemas(true)) as u(x)")).containsExactly(
        List.of("pg_catalog"), List.of(database.getName()));
    // without an alias the column is named after the function, as PostgreSQL names it
    assertThat(rowsOf("select generate_series from pg_database d, generate_series(1, 2)")).containsExactly(List.of(1L), List.of(2L));
  }

  @Test
  void aTableFunctionWithAnArgumentItCannotReadIsDeclinedAndNeverAnsweredEmpty() {
    assertThat(resolve("select s.r from pg_database d, generate_series(1, nope) as s(r)")).isSameAs(PostgresCatalog.DECLINED);
    assertThat(resolve("select s.r from pg_database d, generate_series(1, 2, 0) as s(r)")).isSameAs(PostgresCatalog.DECLINED);
    assertThat(resolve("select s.r from pg_database d, generate_subscripts(1, 2) as s(r)")).isSameAs(PostgresCatalog.DECLINED);
  }

  /** A series is charged to the statement's budget before it is built, so a client cannot ask the catalog for a billion rows. */
  @Test
  void aSeriesTooLongForTheBudgetIsRefusedBeforeItIsBuilt() {
    assertThatThrownBy(() -> resolve("select s.r from pg_database d, generate_series(1, 9223372036854775807) as s(r)"))
        .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    assertThatThrownBy(() -> resolve("select s.r from pg_database d, generate_series(-9223372036854775807, 9223372036854775807) as s(r)"))
        .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
  }

  @Test
  void aTableFunctionIsJoinedToARelation() {
    final List<List<Object>> rows = rowsOf("select t.typname, s.r from pg_type t, generate_series(1, 2) as s(r) where t.oid = 23");
    assertThat(rows).containsExactly(List.of("int4", 1L), List.of("int4", 2L));
  }

  @Test
  void usingJoinsOnTheColumnsBothSidesShare() {
    final List<List<Object>> rows = rowsOf("select ns.nspname, u.x from pg_namespace ns join (select x as nspname, 7 as x from "
        + "unnest(current_schemas(false)) as v(x)) as u using (nspname)");
    assertThat(rows).containsExactly(List.of(database.getName(), 7L));
  }

  @Test
  void anyComparesWithEveryElementOfAnArray() {
    assertThat(rowsOf("select 'a' = any(current_schemas(true)) from pg_database")).containsExactly(List.of(false));
    assertThat(rowsOf("select 'pg_catalog' = any(current_schemas(true)) from pg_database")).containsExactly(List.of(true));
    assertThat(rowsOf("select 'pg_catalog' = some(current_schemas(false)) from pg_database")).containsExactly(List.of(false));
    assertThat(rowsOf("select 'x' <> all(current_schemas(false)) from pg_database")).containsExactly(List.of(true));
    assertThat(rowsOf("select 'pg_catalog' <> all(current_schemas(true)) from pg_database")).containsExactly(List.of(false));
    // an operator other than = and <>, and a NULL operand or element: NULL, as SQL says
    assertThat(rowsOf("select 'b' > any(current_schemas(true)) from pg_database")).containsExactly(List.of(true));
    assertThat(rowsOf("select null = any(current_schemas(true)) from pg_database")).containsExactly(
        Collections.singletonList(null));
    // the elements of an array sub-select
    assertThat(rowsOf("select 23 = any(array(select oid from pg_type where typname = 'int4')) from pg_database")).containsExactly(
        List.of(true));
    assertThat(rowsOf("select nspname from pg_catalog.pg_namespace n, pg_catalog.pg_type t where t.oid = 23 and t.typnamespace = "
        + "n.oid and n.nspname = any(current_schemas(true))")).containsExactly(List.of("pg_catalog"));
  }

  @Test
  void aSecondAliasOfPgTypeReadsItsOwnRow() {
    final List<List<Object>> rows = rowsOf(
        "select t.typname, e.typname from pg_type t join pg_type e on t.typelem = e.oid where t.typname = '_int4'");
    assertThat(rows).containsExactly(List.of("_int4", "int4"));
  }

  @Test
  void theSchemasOnTheSearchPathAreTheCatalogAndTheDatabase() {
    assertThat(rowsOf("select current_schemas(false) from pg_database")).containsExactly(List.of(List.of(database.getName())));
    assertThat(rowsOf("select array_upper(current_schemas(false), 1), array_upper(current_schemas(true), 1) from pg_database"))
        .containsExactly(List.of(1L, 2L));
    assertThat(rowsOf("select (current_schemas(true))[1], (current_schemas(true))[2] from pg_database")).containsExactly(
        List.of("pg_catalog", database.getName()));
    // no dimension of an array has an upper bound but the first
    assertThat(rowsOf("select array_upper(current_schemas(true), 2) from pg_database")).containsExactly(
        Collections.singletonList(null));
  }

  @Test
  void aCountOverTheRelationsOfALookupIsStillItsOneRow() {
    assertThat(rowsOf("select count(*) from pg_catalog.pg_range")).containsExactly(List.of(0L));
    assertThat(rowsOf("select count(*) from pg_catalog.pg_enum")).containsExactly(List.of(0L));
  }
}
