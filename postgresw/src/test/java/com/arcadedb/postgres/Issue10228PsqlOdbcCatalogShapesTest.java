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
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10228: the catalog queries psql 17 ({@code \dt}, {@code \d "Person"}) and psqlodbc
 * ({@code SQLColumns}, {@code SQLTables(SQL_ALL_TABLE_TYPES)}) send came back empty, unfiltered or as a syntax
 * error. Every statement below is the exact text those clients sent to a PostgreSQL 17 server, captured with
 * {@code log_statement=all}, and every expected answer is what PostgreSQL 17 answered for the same schema (minus
 * its own system relations, which this catalog does not have).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10228PsqlOdbcCatalogShapesTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10228PsqlOdbcCatalogShapesTest";
  private static final String USER          = "root";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> {
      database.getSchema().createDocumentType("Person").createProperty("id", Type.INTEGER);
      database.getSchema().getType("Person").createProperty("name", Type.STRING);
      database.getSchema().createDocumentType("Note");
      database.getSchema().createVertexType("City");
    });
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  // ---------------------------------------------------------------- case 1: a parenthesised join in FROM

  @Test
  void aParenthesisedJoinIsReadAsTheJoinItWraps() {
    final PostgresCatalog.Answer answer = resolve(
        "select 1 from (pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace)");

    assertThat(answer.rows).hasSize(3);
  }

  /** psqlodbc's {@code PGAPI_Columns} for {@code SQLColumns(table = "Person")}: the name filter is in an ON clause. */
  @Test
  void odbcColumnsOfOneTableReadTheFilterInTheJoinCondition() {
    // A second type with a column, so that a filter that is not applied shows.
    database.transaction(() -> database.getSchema().getType("Note").createProperty("text", Type.STRING));

    final PostgresCatalog.Answer answer = resolve(odbcColumns(
        " and c.relname like 'Person' and n.nspname like '" + database.getName() + "'"));

    assertThat(column(answer, "relname")).containsExactly("Person", "Person");
    assertThat(column(answer, "attname")).containsExactly("id", "name");
    assertThat(column(answer, "typname")).containsExactly("int4", "varchar");
    assertThat(column(answer, "attnum")).containsExactly(1, 2);
  }

  /** The same with psqlodbc's exact-match spelling, {@code OPERATOR(pg_catalog.=)}. */
  @Test
  void odbcColumnsWithTheOperatorSpellingOfEquality() {
    final PostgresCatalog.Answer answer = resolve(odbcColumns(
        " and c.relname operator(pg_catalog.=) 'Person' and n.nspname operator(pg_catalog.=) '" + database.getName() + "'"));

    assertThat(column(answer, "attname")).containsExactly("id", "name");
  }

  /** {@code SQLColumns()} with no filter: every column of every type, and nothing for a type with no properties. */
  @Test
  void odbcColumnsOfEveryTable() {
    final PostgresCatalog.Answer answer = resolve(odbcColumns(""));

    assertThat(column(answer, "attname")).containsExactly("id", "name");
  }

  @Test
  void odbcColumnsOfATableThatDoesNotExistAreNone() {
    final PostgresCatalog.Answer answer = resolve(odbcColumns(" and c.relname like 'Nope'"));

    assertThat(answer.rows).isEmpty();
    assertThat(answer.columns).hasSize(18);
  }

  // ---------------------------------------------------------------- case 2: a join to pg_am

  @Test
  void aLeftJoinToPgAmKeepsEveryTable() {
    final PostgresCatalog.Answer answer = resolve(
        "select c.relname, am.amname from pg_catalog.pg_class c left join pg_catalog.pg_am am on am.oid = c.relam");

    assertThat(column(answer, "relname")).containsExactly("City", "Note", "Person");
    assertThat(column(answer, "amname")).containsExactly("heap", "heap", "heap");
  }

  /** psql 17's {@code \dt}, verbatim. */
  @Test
  void psqlListTables() {
    final PostgresCatalog.Answer answer = resolve("""
        SELECT n.nspname as "Schema",
          c.relname as "Name",
          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
          pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE c.relkind IN ('r','p','')
              AND n.nspname <> 'pg_catalog'
              AND n.nspname !~ '^pg_toast'
              AND n.nspname <> 'information_schema'
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 1,2;""");

    assertThat(column(answer, "Name")).containsExactly("City", "Note", "Person");
    assertThat(column(answer, "Type")).containsExactly("table", "table", "table");
    assertThat(column(answer, "Owner")).containsExactly(USER, USER, USER);
    assertThat(column(answer, "Schema")).containsOnly(database.getName());
  }

  // ---------------------------------------------------------------- case 3: OPERATOR(pg_catalog.=)

  @Test
  void theOperatorSpellingOfEqualityFilters() {
    assertThat(resolve("select relname from pg_catalog.pg_class where relname operator(pg_catalog.=) 'zzz'").rows).isEmpty();
    assertThat(column(resolve("select relname from pg_catalog.pg_class where relname OPERATOR(pg_catalog.=) 'Note'"),
        "relname")).containsExactly("Note");
    assertThat(column(resolve("select relname from pg_class where relname operator(<>) 'Note'"), "relname"))
        .containsExactly("City", "Person");
  }

  /** psql's {@code \d "Person"} looks the relation up with a regular expression and a COLLATE clause. */
  @Test
  void psqlDescribeLooksUpOnlyTheNamedRelation() {
    final PostgresCatalog.Answer answer = resolve("""
        SELECT c.oid,
          n.nspname,
          c.relname
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname OPERATOR(pg_catalog.~) '^(Person)$' COLLATE pg_catalog.default
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 2, 3;""");

    assertThat(column(answer, "relname")).containsExactly("Person");
    assertThat(column(answer, "oid")).containsExactly(personOid());
  }

  /** The second query of {@code \d "Person"}, whose empty answer made psql print "Did not find any relation with OID". */
  @Test
  void psqlDescribeReadsTheTableProperties() {
    final PostgresCatalog.Answer answer = resolve("""
        SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition, '', c.reltablespace, CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END, c.relpersistence, c.relreplident, am.amname
        FROM pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
        LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
        WHERE c.oid = '%d';""".formatted(personOid()));

    assertThat(answer.rows).hasSize(1);
    // 15 columns, two of them named "?column?": a row map cannot hold both under one key.
    assertThat(answer.columns).hasSize(15);
    final List<String> announced = new ArrayList<>();
    for (final String key : answer.columns.keySet())
      announced.add(PostgresCatalog.announcedName(key));
    assertThat(announced).containsExactly("relchecks", "relkind", "relhasindex", "relhasrules", "relhastriggers",
        "relrowsecurity", "relforcerowsecurity", "relhasoids", "relispartition", "?column?", "reltablespace", "?column?",
        "relpersistence", "relreplident", "amname");

    final List<Object> row = new ArrayList<>(answer.rows.get(0).values());
    assertThat(row.get(1)).isEqualTo("r");
    assertThat(row.get(7)).isEqualTo(Boolean.FALSE);
    assertThat(row.get(9)).isEqualTo("");
    assertThat(row.get(11)).isEqualTo("");
    assertThat(row.get(12)).isEqualTo("p");
    assertThat(row.get(13)).isEqualTo("d");
    assertThat(row.get(14)).isEqualTo("heap");
  }

  /** The third query of {@code \d "Person"}: the columns, with two correlated scalar sub-selects per row. */
  @Test
  void psqlDescribeReadsTheColumnsThroughCorrelatedSubqueries() {
    final PostgresCatalog.Answer answer = resolve("""
        SELECT a.attname,
          pg_catalog.format_type(a.atttypid, a.atttypmod),
          (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
           FROM pg_catalog.pg_attrdef d
           WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),
          a.attnotnull,
          (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t
           WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation,
          a.attidentity,
          a.attgenerated
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = '%d' AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum;""".formatted(personOid()));

    assertThat(column(answer, "attname")).containsExactly("id", "name");
    assertThat(column(answer, "format_type")).containsExactly("integer", "character varying");
    assertThat(column(answer, "?column?")).containsExactly(null, null);
    assertThat(column(answer, "attcollation")).containsExactly(null, null);
    assertThat(column(answer, "attnotnull")).containsExactly(false, false);
  }

  /** A column default is what the pg_attrdef sub-select of {@code \d} reads, as information_schema already says. */
  @Test
  void aColumnDefaultIsReadThroughPgAttrdef() {
    database.transaction(() -> database.getSchema().getType("Note").createProperty("title", Type.STRING).setDefaultValue("'untitled'"));

    final PostgresCatalog.Answer answer = resolve("""
        SELECT a.attname,
          (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)
           FROM pg_catalog.pg_attrdef d
           WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef) AS "default"
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = '%d' AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum;""".formatted(oidOf("Note")));

    assertThat(column(answer, "attname")).containsExactly("title");
    assertThat(column(answer, "default")).containsExactly(
        column(resolve("select column_default from information_schema.columns where table_name = 'Note'"),
            "column_default").get(0));
    assertThat(column(answer, "default").get(0)).isNotNull();

    // pgjdbc's getColumns() reads COLUMN_DEF through a LEFT JOIN to pg_attrdef, which a column without one nulls.
    final PostgresCatalog.Answer jdbc = resolve("SELECT c.relname, a.attname, pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc "
        + "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid) "
        + "LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum) ORDER BY 1, 2");
    assertThat(column(jdbc, "attname")).containsExactly("title", "id", "name");
    assertThat(column(jdbc, "adsrc")).containsExactly(column(answer, "default").get(0), null, null);

    // and an INNER JOIN to it keeps only the columns that have one
    assertThat(column(resolve("SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_attrdef d "
        + "ON d.adrelid = a.attrelid AND d.adnum = a.attnum"), "attname")).containsExactly("title");
  }

  // ---------------------------------------------------------------- case 4: UNION

  @Test
  void aUnionOfLiteralsIsAnswered() {
    final PostgresCatalog.Answer answer = resolve("select 1 union select 2");

    assertThat(answer.rows).hasSize(2);
    assertThat(column(answer, "?column?")).containsExactly(1L, 2L);
  }

  @Test
  void setOperationsFollowPostgresSemantics() {
    assertThat(column(resolve("select 1 as x union select 1 union select 2"), "x")).containsExactly(1L, 2L);
    assertThat(column(resolve("select 1 as x union all select 1 union all select 2"), "x")).containsExactly(1L, 1L, 2L);
    assertThat(column(resolve("select 1 as x union select 2 union select 3 except select 2"), "x")).containsExactly(1L, 3L);
    // INTERSECT binds tighter than UNION.
    assertThat(column(resolve("select 1 as x union select 2 intersect select 3"), "x")).containsExactly(1L);
    assertThat(column(resolve("select 3 as x union select 1 union select 2 order by 1"), "x")).containsExactly(1L, 2L, 3L);
    assertThat(column(resolve("select 3 as x union select 1 union select 2 order by x desc limit 2"), "x"))
        .containsExactly(3L, 2L);
    assertThat(column(resolve("(select 1 as x) union (select 2)"), "x")).containsExactly(1L, 2L);
  }

  /** psqlodbc's {@code SQLTables(SQL_ALL_TABLE_TYPES)}, verbatim: three columns, two of them unnamed. */
  @Test
  void odbcAllTableTypes() {
    final PostgresCatalog.Answer answer = resolve(
        "select NULL, NULL, relkind from (select 'r' as relkind union select 'v' union select 'm' union select 'f' union select 'p') as a");

    assertThat(answer.columns).hasSize(3);
    assertThat(column(answer, "relkind")).containsExactly("r", "v", "m", "f", "p");
    for (final Map<String, Object> row : answer.rows)
      assertThat(new ArrayList<>(row.values()).subList(0, 2)).containsExactly(null, null);
  }

  /**
   * Two output columns named alike are each announced under that name, and a reference to the name is ambiguous, as
   * in PostgreSQL: ORDER BY and GROUP BY by position or by a name only one column has keep working, a reference to
   * the shared name is declined rather than resolved to whichever column comes first.
   */
  @Test
  void duplicateColumnNamesInOrderByAndGroupBy() {
    final String types = "(select NULL, NULL, relkind from (select 'r' as relkind union select 'v' union select 'm') b)";

    assertThat(column(resolve("select NULL, NULL, relkind from (select 'r' as relkind union select 'v' union select 'm') a "
        + "order by 3"), "relkind")).containsExactly("m", "r", "v");
    assertThat(column(resolve("select NULL, NULL, relkind from (select 'r' as relkind union select 'v' union select 'm') a "
        + "order by relkind desc"), "relkind")).containsExactly("v", "r", "m");
    assertThat(column(resolve("select relkind, count(*) as n from " + types + " a group by relkind order by 1"), "n"))
        .containsExactly(1L, 1L, 1L);
    assertThat(column(resolve("select relkind from " + types + " a group by 1 order by 1"), "relkind"))
        .containsExactly("m", "r", "v");

    // Over a catalog relation an ambiguous reference is declined; over literals alone the statement is the SQL
    // engine's, which is told so by a null answer.
    final String catalog = "(select NULL, NULL, relname from pg_class)";
    assertThat(PostgresCatalog.resolve("select NULL, NULL, relname from pg_class order by \"?column?\"", database, USER))
        .isSameAs(PostgresCatalog.DECLINED);
    assertThat(PostgresCatalog.resolve("select count(*) from " + catalog + " a group by \"?column?\"", database, USER))
        .isSameAs(PostgresCatalog.DECLINED);
    assertThat(PostgresCatalog.resolve("select \"?column?\" from " + catalog + " a", database, USER))
        .isSameAs(PostgresCatalog.DECLINED);
    assertThat(column(resolve("select relname from " + catalog + " a order by relname"), "relname"))
        .containsExactly("City", "Note", "Person");
    assertThat(PostgresCatalog.resolve("select NULL, NULL, relkind from (select 'r' as relkind union select 'v') a "
        + "order by \"?column?\"", database, USER)).isNull();
    assertThat(PostgresCatalog.resolve("select \"?column?\" from " + types + " a", database, USER)).isNull();
  }

  /**
   * The key of a second column named alike holds a NUL, which no client-written name can: a wire query is a C string.
   * A look-alike alias is its own column, and the unnamed ones around it keep theirs.
   */
  @Test
  void aLookAlikeAliasDoesNotCollideWithADuplicateColumnKey() {
    final PostgresCatalog.Answer answer = resolve(
        "select NULL, 2 as \"?column?2\", NULL, relname from pg_class where relname = 'Note'");

    assertThat(answer.columns).hasSize(4);
    final List<String> announced = new ArrayList<>();
    for (final String key : answer.columns.keySet())
      announced.add(PostgresCatalog.announcedName(key));
    assertThat(announced).containsExactly("?column?", "?column?2", "?column?", "relname");
    assertThat(column(answer, "?column?2")).containsExactly(2L);
    assertThat(column(answer, "relname")).containsExactly("Note");
  }

  /** A union of a catalog relation and a literal arm is answered too. */
  @Test
  void aUnionMixingACatalogRelationAndLiterals() {
    final PostgresCatalog.Answer answer = resolve(
        "select relname from pg_class where relname = 'Note' union all select 'extra' order by 1");

    assertThat(column(answer, "relname")).containsExactly("Note", "extra");
  }

  @Test
  void bagSetOperationsAndLimitAll() {
    // INTERSECT binds first: "select 2 intersect all select 1" is empty (PostgreSQL 17 answers 1,1 too).
    assertThat(column(resolve("select 1 as x union all select 1 union all select 2 intersect all select 1"), "x"))
        .containsExactly(1L, 1L);
    assertThat(column(resolve("select 1 as x union all select 1 union all select 1 except all select 1"), "x"))
        .containsExactly(1L, 1L);
    assertThat(column(resolve("select 1 as x union all select 1 except select 1"), "x")).isEmpty();
    assertThat(column(resolve("select 1 as x union all select 1 intersect all select 1"), "x")).containsExactly(1L, 1L);
    assertThat(column(resolve("select 2 as x union select 1 order by 1 limit all"), "x")).containsExactly(1L, 2L);
    assertThat(column(resolve("select relname from pg_class order by 1 limit all"), "relname"))
        .containsExactly("City", "Note", "Person");
  }

  /** Numbers compare by value when a set operation removes duplicates, as in SQL: 1 and 1.0 are one row. */
  @Test
  void setOperationsCompareNumbersByValue() {
    assertThat(resolve("select 1 as x union select 1.0").rows).hasSize(1);
    assertThat(resolve("select 1 as x union select 1.5").rows).hasSize(2);
  }

  /** A COPY header announces a duplicate column under its own name, as the RowDescription does. */
  @Test
  void aCopyHeaderAnnouncesDuplicateColumnsByName() {
    final PostgresCatalog.Answer answer = resolve(
        "select NULL, NULL, relkind from (select 'r' as relkind union select 'v') as a");
    final StringBuilder header = new StringBuilder();
    PostgresCopyStatement.parse("COPY (select 1) TO STDOUT WITH (FORMAT csv, HEADER true)")
        .appendHeader(header, answer.columns.keySet());

    assertThat(header.toString()).isEqualTo("?column?,?column?,relkind\n");

    // FORCE_QUOTE names a column the way the client announced it, so a duplicate is forced like its first.
    final StringBuilder row = new StringBuilder();
    PostgresCopyStatement.parse("COPY (select 1) TO STDOUT WITH (FORMAT csv, FORCE_QUOTE (\"?column?\"))")
        .appendRow(row, new String[] { "a", "b", "r" }, answer.columns.keySet().toArray(new String[0]));
    assertThat(row.toString()).isEqualTo("\"a\",\"b\",r\n");
  }

  /** A UNION whose arm reads the user's own type is not this catalog's question: the SQL engine answers it. */
  @Test
  void aUnionOverAUserTypeIsNotACatalogQuery() {
    // not even partly, when the other arm is a catalog relation
    assertThat(PostgresCatalog.resolve("select relname from pg_class union select name from Person", database, USER))
        .isNull();
    // and a word that only contains a set-operation keyword does not make a query one
    assertThat(PostgresCatalog.mightBeAnswered("select exceptions, reunion from Person")).isFalse();
    assertThat(PostgresCatalog.mightBeAnswered("select 1 UNION select 2")).isTrue();
    assertThat(PostgresCatalog.resolve("select name from Person union select 'x'", database, USER)).isNull();
    // and a query that merely contains the word is not touched at all
    assertThat(PostgresCatalog.resolve("select name from Person where name = 'union'", database, USER)).isNull();
    // nor a literal union this catalog cannot evaluate
    assertThat(PostgresCatalog.resolve("select now() union select now()", database, USER)).isNull();
  }

  // ---------------------------------------------------------------- case 5: count(*)

  @Test
  void countOverACatalogRelation() {
    assertThat(column(resolve("select count(*) from pg_catalog.pg_class c"), "count")).containsExactly(3L);
    assertThat(column(resolve("select count(*) from pg_class c where c.relname = 'zzz'"), "count")).containsExactly(0L);
    assertThat(column(resolve("select count(*) as n, min(relname), max(relname) from pg_class"), "n")).containsExactly(3L);
    assertThat(column(resolve("select count(*) as n, min(relname), max(relname) from pg_class"), "min"))
        .containsExactly("City");
    assertThat(column(resolve("select count(attname) from pg_attribute"), "count")).containsExactly(2L);
    assertThat(column(resolve("select count(distinct attrelid) from pg_attribute"), "count")).containsExactly(1L);
  }

  @Test
  void groupByOverACatalogRelation() {
    final PostgresCatalog.Answer answer = resolve(
        "select c.relname, count(*) as columns from pg_class c join pg_attribute a on a.attrelid = c.oid group by c.relname");

    assertThat(column(answer, "relname")).containsExactly("Person");
    assertThat(column(answer, "columns")).containsExactly(2L);
  }

  // ---------------------------------------------------------------- the join semantics the fixes rest on

  /** A LEFT JOIN whose condition does not hold reads NULL from the joined relation, and keeps the row. */
  @Test
  void aLeftJoinThatDoesNotMatchReadsNull() {
    final PostgresCatalog.Answer answer = resolve(
        "select c.relname, n.nspname from pg_class c left join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'other'");

    assertThat(column(answer, "relname")).containsExactly("City", "Note", "Person");
    assertThat(column(answer, "nspname")).containsExactly(null, null, null);
  }

  /** An INNER JOIN's condition is a filter, and a condition that does not hold removes the row. */
  @Test
  void anInnerJoinConditionFilters() {
    final PostgresCatalog.Answer answer = resolve(
        "select c.relname from pg_class c inner join pg_namespace n on n.oid = c.relnamespace and c.relname = 'City'");

    assertThat(column(answer, "relname")).containsExactly("City");
  }

  @Test
  void existsAndInSubqueries() {
    assertThat(column(resolve(
        "select c.relname from pg_class c where exists (select 1 from pg_attribute a where a.attrelid = c.oid)"),
        "relname")).containsExactly("Person");
    assertThat(column(resolve(
        "select c.relname from pg_class c where c.oid in (select a.attrelid from pg_attribute a)"),
        "relname")).containsExactly("Person");
    assertThat(column(resolve(
        "select c.relname from pg_class c where c.oid not in (select a.attrelid from pg_attribute a)"),
        "relname")).containsExactly("City", "Note");
    assertThat(column(resolve(
        "select c.relname, array(select a.attname from pg_attribute a where a.attrelid = c.oid) as cols from pg_class c where c.relname = 'Person'"),
        "cols")).containsExactly(List.of("id", "name"));
  }

  /** A scalar sub-select answering more than one row is an error in PostgreSQL, so it is not answered. */
  @Test
  void aScalarSubqueryWithMoreThanOneRowIsDeclined() {
    assertThat(PostgresCatalog.resolve("select (select relname from pg_class) from pg_namespace", database, USER))
        .isSameAs(PostgresCatalog.DECLINED);
  }

  /** Nested sub-selects are bounded: a deep enough nesting is declined rather than recursing until the stack gives out. */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void deeplyNestedSubqueriesAreDeclinedNotOverflowed() {
    final StringBuilder query = new StringBuilder("select relname from pg_class");
    for (int i = 0; i < 5_000; i++)
      query.insert(0, "select * from (").append(") x").append(i);

    assertThat(PostgresCatalog.resolve(query.toString(), database, USER)).isSameAs(PostgresCatalog.DECLINED);
  }

  // ---------------------------------------------------------------- what the rewrites and the evaluator must not do

  /** The OPERATOR and COLLATE rewrites work on tokens: a string literal or a quoted identifier is left as written. */
  @Test
  void theSpellingRewritesLeaveLiteralsAndQuotedIdentifiersAlone() {
    final PostgresCatalog.Answer answer = resolve("select 'a OPERATOR(pg_catalog.=) b' as op, 'x COLLATE \"C\"' as col, "
        + "'~~' as tilde, relname as \"collate\" from pg_class where relname = 'Note'");

    assertThat(column(answer, "op")).containsExactly("a OPERATOR(pg_catalog.=) b");
    assertThat(column(answer, "col")).containsExactly("x COLLATE \"C\"");
    assertThat(column(answer, "tilde")).containsExactly("~~");
    assertThat(column(answer, "collate")).containsExactly("Note");
  }

  /** COLLATE with a quoted or a schema-qualified quoted name is dropped whole, not half. */
  @Test
  void collateWithAQuotedNameIsDroppedWhole() {
    assertThat(column(resolve("select relname from pg_class where relname = 'Note' COLLATE \"C\""), "relname"))
        .containsExactly("Note");
    assertThat(column(resolve("select relname from pg_class where relname = 'Note' COLLATE pg_catalog.\"default\" "
        + "order by relname collate \"C\""), "relname")).containsExactly("Note");
  }

  /**
   * An operator this catalog does not implement, spelled through OPERATOR(...), becomes that operator rather than a
   * different one: unreadable, so it keeps every row of the permissive families and declines over pg_type.
   */
  @Test
  void anUnknownOperatorSpelledThroughOperatorIsUnreadable() {
    assertThat(column(resolve("select relname from pg_class where relname operator(pg_catalog.@@) 'x'"), "relname"))
        .containsExactly("City", "Note", "Person");
    assertThat(PostgresCatalog.resolve("select typname from pg_type where typname operator(pg_catalog.@@) 'x'", database,
        USER)).isSameAs(PostgresCatalog.DECLINED);
  }

  /** A statement wrapped in parentheses many thousand times is unwrapped in one pass and answered. */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS) // a hang detector: peeling one layer per scan is quadratic
  void aDeeplyParenthesisedStatementIsUnwrappedInOnePass() {
    final int layers = 50_000;
    final String query = "(".repeat(layers) + "select relname from pg_class" + ")".repeat(layers) + " union select 'x'";
    final StallAwareStopwatch unwrap = StallAwareStopwatch.start();
    assertThat(column(resolve(query), "relname")).containsExactly("City", "Note", "Person", "x");
    unwrap.assertGaveUpWithin(15_000, "one scan of the parentheses from one scan per layer, 2.5 * 10^9 token reads");
  }

  /** A set operation naming no catalog relation is a SELECT: an INSERT that only contains the word is not one. */
  @Test
  void anInsertContainingASetOperationWordIsNotTokenized() {
    assertThat(PostgresCatalog.resolve("insert into Person set name = 'select 1 union select 2'", database, USER)).isNull();
  }

  /** The nesting bound is exact: 31 derived tables deep is answered, 33 is declined. */
  @Test
  void theNestingBoundIsExact() {
    assertThat(PostgresCatalog.resolve(derivedTables(31), database, USER).rows).hasSize(3);
    assertThat(PostgresCatalog.resolve(derivedTables(33), database, USER)).isSameAs(PostgresCatalog.DECLINED);
  }

  private static String derivedTables(final int depth) {
    final StringBuilder query = new StringBuilder("select relname from pg_class");
    for (int i = 0; i < depth; i++)
      query.insert(0, "select * from (").append(") x").append(i);
    return query.toString();
  }

  /** POSIX character classes mean what they mean in PostgreSQL, not the bracket set Java would read them as. */
  @Test
  void posixCharacterClassesAreTranslated() {
    assertThat(column(resolve("select relname from pg_class where relname ~ '^[[:upper:]][[:lower:]]+$'"), "relname"))
        .containsExactly("City", "Note", "Person");
    assertThat(resolve("select relname from pg_class where relname ~ '^[[:digit:]]+$'").rows).isEmpty();
    assertThat(resolve("select relname from pg_class where relname ~ '[^[:alpha:]]'").rows).isEmpty();
    assertThat(column(resolve("select relname from pg_class where relname ~ '^[[:word:]]+$'"), "relname"))
        .containsExactly("City", "Note", "Person");
    assertThat(column(resolve("select relname from pg_class where relname ~ '^[[:ascii:]]+$'"), "relname"))
        .containsExactly("City", "Note", "Person");
    assertThat(column(resolve("select relname from pg_class where relname ~ '[[:<:]]Note[[:>:]]'"), "relname"))
        .containsExactly("Note");
    // Outside a bracket expression "[:word:]" is the set of the characters ":word", in PostgreSQL as in Java
    // (PostgreSQL 17 gives the same answers to both of these).
    assertThat(resolve("select relname from pg_class where relname ~ '^[:word:]+$'").rows).isEmpty();
    assertThat(column(resolve("select relname from pg_class where relname ~ '^[[:upper:]][:lowert]+$'"), "relname"))
        .containsExactly("Note");
    // '&' and '[' inside a bracket are literals in POSIX: "[a&&b]" is the set {a, &, b}, not Java's intersection.
    assertThat(column(resolve("select relname from pg_class where relname ~ '^[CN&&x]'"), "relname"))
        .containsExactly("City", "Note");
    // and a '[' inside a bracket is a literal: "^[a[b]c]$" is the set {a, [, b} followed by "c]" (PostgreSQL 17 agrees)
    final Pattern nested = Pattern.compile(PostgresCatalogExpression.javaRegex("^[a[b]c]$"));
    assertThat(nested.matcher("ac]").matches()).isTrue();
    assertThat(nested.matcher("[c]").matches()).isTrue();
    assertThat(nested.matcher("c").matches()).isFalse();
    // A ']' first in a bracket is a member, also after '^', and a backslash escapes inside one (PostgreSQL 17 agrees)
    assertThat(Pattern.compile(PostgresCatalogExpression.javaRegex("^[^]]$")).matcher("]").matches()).isFalse();
    assertThat(Pattern.compile(PostgresCatalogExpression.javaRegex("^[^]]$")).matcher("x").matches()).isTrue();
    assertThat(Pattern.compile(PostgresCatalogExpression.javaRegex("^[]a]$")).matcher("]").matches()).isTrue();
    assertThat(Pattern.compile(PostgresCatalogExpression.javaRegex("^[]a]$")).matcher("b").matches()).isFalse();
    assertThat(Pattern.compile(PostgresCatalogExpression.javaRegex("^[\\\\a]$")).matcher("\\").matches()).isTrue();
    assertThat(Pattern.compile(PostgresCatalogExpression.javaRegex("^[\\\\a]$")).matcher("b").matches()).isFalse();
    // A class this catalog does not translate is unreadable, not misread: permissive here, declined over pg_type.
    assertThat(column(resolve("select relname from pg_class where relname ~ '^[[:nope:]]+$'"), "relname"))
        .containsExactly("City", "Note", "Person");
    assertThat(PostgresCatalog.resolve("select typname from pg_type where typname ~ '^[[:nope:]]+$'", database, USER))
        .isSameAs(PostgresCatalog.DECLINED);
    // Untranslated, Java reads [[:alpha:]] as the character set "[:alph" and matches none of these names.
    assertThat(column(resolve("select relname from pg_class where relname ~ '^[[:alpha:]]+$'"), "relname"))
        .containsExactly("City", "Note", "Person");
  }

  /** A pattern Java cannot compile is not an answer: kept by the permissive filter, declined over pg_type. */
  @Test
  void anInvalidRegularExpressionIsUnknown() {
    assertThat(column(resolve("select relname from pg_class where relname ~ '(['"), "relname"))
        .containsExactly("City", "Note", "Person");
    assertThat(PostgresCatalog.resolve("select typname from pg_type where typname ~ '(['", database, USER))
        .isSameAs(PostgresCatalog.DECLINED);
  }

  /** A catastrophically backtracking pattern stops after a bounded amount of work, rather than spinning per row. */
  @Test
  void aBacktrackingRegularExpressionIsBounded() {
    // java.util.regex memoizes the classic exponential shapes, but not this polynomial one: unbounded, it reads
    // the 40 characters of the name some 10^8 times.
    final String name = "x".repeat(40);
    database.transaction(() -> database.getSchema().createDocumentType(name));

    // Refused rather than read as UNKNOWN, which the permissive filter would keep: a name filter that ran out of
    // budget must not answer a row that does not match it, and an empty answer would look like an empty catalog.
    final StallAwareStopwatch match = StallAwareStopwatch.start();
    assertThatThrownBy(() -> PostgresCatalog.resolve("select relname from pg_class where relname ~ '^(.*)(.*)(.*)(.*)(.*)Y$'",
        database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    match.assertGaveUpWithin(15_000, "a match cut off by its step budget from one reading the name some 10^8 times");
    // A correlated sub-select re-parses into fresh nodes per outer row; the statement budget still bounds it.
    assertThatThrownBy(() -> PostgresCatalog.resolve("select c.relname from pg_class c where exists (select 1 "
        + "from pg_class d where d.relname ~ '^(.*)(.*)(.*)(.*)(.*)Y$' and d.oid = c.oid)", database, USER))
        .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    // A set operation naming no catalog relation is the SQL engine's to answer: over its budget it is handed on,
    // not refused.
    assertThat(PostgresCatalog.resolve("select 1 as x where '" + name + "' ~ '^(.*)(.*)(.*)(.*)(.*)Y$' union select 2",
        database, USER)).isNull();
    assertThat(PostgresCatalog.resolve("select 1 union select 2", database, USER)).isNotNull();
    // The same pattern over names it is cheap on is answered: the conjunct before it excludes the long name first.
    assertThat(column(resolve("select relname from pg_class where relname <> '" + name + "' "
        + "and relname !~ '^(.*)(.*)(.*)(.*)(.*)Y$'"), "relname")).containsExactly("City", "Note", "Person");
  }

  /** A second alias read in an inner join's condition would read the first alias's row: declined, not answered wrong. */
  @Test
  void aSecondAliasReadOnlyInAnInnerJoinConditionIsDeclined() {
    assertThat(PostgresCatalog.resolve("select c.relname from pg_class c join pg_class p on p.relname = 'Person'",
        database, USER)).isSameAs(PostgresCatalog.DECLINED);
  }

  /** OFFSET and FETCH after a set operation's ORDER BY are outside the slice: never folded into the sort key. */
  @Test
  void anOffsetAfterASetOperationIsNotAnswered() {
    assertThat(PostgresCatalog.resolve("select 1 as x union select 2 order by 1 offset 1", database, USER)).isNull();
    assertThat(PostgresCatalog.resolve("select relname from pg_class union select 'x' order by 1 fetch first 1 rows only",
        database, USER)).isSameAs(PostgresCatalog.DECLINED);
  }

  /** A join condition with an unreadable conjunct still filters on its readable ones, as a WHERE clause does. */
  @Test
  void anUnreadableConjunctInAJoinConditionDoesNotDisableTheReadableOnes() {
    final PostgresCatalog.Answer answer = resolve("select c.relname from pg_class c join pg_namespace n "
        + "on n.oid = c.relnamespace and c.relname is distinct from 'x' and c.relname = 'City'");

    assertThat(column(answer, "relname")).containsExactly("City");

    // and an ON condition with nothing readable left in it keeps every row: a superset, never a wrong subset
    assertThat(column(resolve("select c.relname from pg_class c join pg_namespace n on c.relname is distinct from 'x'"),
        "relname")).containsExactly("City", "Note", "Person");
  }

  /** One regular-expression predicate is bounded over all the rows of the statement, not only per row. */
  @Test
  void aBacktrackingRegularExpressionIsBoundedAcrossRows() {
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.getSchema().createDocumentType("x".repeat(40) + i);
    });

    final StallAwareStopwatch rows = StallAwareStopwatch.start();
    assertThatThrownBy(() -> PostgresCatalog.resolve("select relname from pg_class where relname ~ '^(.*)(.*)(.*)(.*)(.*)Y$'",
        database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    rows.assertGaveUpWithin(15_000, "one budget for the predicate over every row from a full match budget per row");

    // The same pattern in a sub-select correlated to every one of those rows: each outer row re-parses it into a fresh
    // node with a fresh regex budget, and the statement is still refused within the bound - the per-match budget cuts
    // each match, and the statement's work budget the number of re-parses (deeplyNestedSubSelectsAndJoins...).
    final StallAwareStopwatch correlated = StallAwareStopwatch.start();
    assertThatThrownBy(() -> PostgresCatalog.resolve("select c.relname from pg_class c where exists (select 1 from "
        + "pg_class d where d.oid = c.oid and d.relname ~ '^(.*)(.*)(.*)(.*)(.*)Y$')", database, USER))
        .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    correlated.assertGaveUpWithin(15_000, "the statement work budget from a fresh regex budget per outer row");
  }

  /** string_agg's delimiter is the one of the row whose value follows it, as in PostgreSQL. */
  @Test
  void stringAggReadsTheDelimiterOfEachRow() {
    assertThat(column(resolve("select string_agg(relname, relname) as s from pg_class"), "s"))
        .containsExactly("CityNoteNotePersonPerson");
    assertThat(column(resolve("select string_agg(relname, ', ') as s from pg_class"), "s"))
        .containsExactly("City, Note, Person");
  }

  /**
   * A join never multiplies rows here: each family is enumerated once and a join only decorates its rows, so a
   * cross join of a relation with itself - which would be a product - is declined rather than built.
   */
  @Test
  void aSelfCrossJoinIsDeclinedNotMultiplied() {
    assertThat(PostgresCatalog.resolve("select a.attname, b.attname from pg_attribute a, pg_attribute b", database, USER))
        .isSameAs(PostgresCatalog.DECLINED);
    assertThat(PostgresCatalog.resolve("select a.attname, b.attname from pg_attribute a cross join pg_attribute b",
        database, USER)).isSameAs(PostgresCatalog.DECLINED);
  }

  @Test
  void anOuterJoinToAParenthesisedJoinOrAnAliasedOneIsDeclined() {
    assertThat(PostgresCatalog.resolve("select c.relname from pg_class c left join (pg_attribute a join pg_type t "
        + "on t.oid = a.atttypid) on a.attrelid = c.oid", database, USER)).isSameAs(PostgresCatalog.DECLINED);
    assertThat(PostgresCatalog.resolve("select 1 from (pg_class c join pg_namespace n on n.oid = c.relnamespace) j",
        database, USER)).isSameAs(PostgresCatalog.DECLINED);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS) // a hang detector: unbounded, this is 3^32 evaluations
  void deeplyNestedSubSelectsAndJoinsAreDeclinedNotOverflowed() {
    final StringBuilder exists = new StringBuilder("select 1 from pg_class");
    for (int i = 0; i < 5_000; i++)
      exists.insert(0, "select 1 from pg_class where exists (").append(")");
    // Each level runs once per row of the one around it, so the depth bound alone would still leave 3^32
    // evaluations: the work budget is what makes this return, and spending it refuses the statement.
    final StallAwareStopwatch nested = StallAwareStopwatch.start();
    assertThatThrownBy(() -> PostgresCatalog.resolve(exists.toString(), database, USER))
        .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    nested.assertGaveUpWithin(15_000, "the statement work budget from 3^32 correlated evaluations");

    final StringBuilder joins = new StringBuilder("pg_class c");
    for (int i = 0; i < 5_000; i++)
      joins.insert(0, "(").append(" join pg_namespace n").append(i).append(" on n").append(i).append(".oid = c.relnamespace)");
    assertThat(PostgresCatalog.resolve("select 1 from " + joins, database, USER)).isSameAs(PostgresCatalog.DECLINED);
  }

  // ---------------------------------------------------------------- helpers

  private static String odbcColumns(final String tableFilter) {
    return "select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull, "
        + "c.relhasrules, c.relkind, c.oid, pg_get_expr(d.adbin, d.adrelid), case t.typtype when 'd' then t.typbasetype else 0 end, "
        + "t.typtypmod, 0, attidentity, c.relhassubclass from (((pg_catalog.pg_class c inner join pg_catalog.pg_namespace n "
        + "on n.oid = c.relnamespace" + tableFilter + ") inner join pg_catalog.pg_attribute a on (not a.attisdropped) "
        + "and a.attnum operator(pg_catalog.>) 0 and a.attrelid operator(pg_catalog.=) c.oid) inner join pg_catalog.pg_type t "
        + "on t.oid operator(pg_catalog.=) a.atttypid) left outer join pg_attrdef d on a.atthasdef and d.adrelid "
        + "operator(pg_catalog.=) a.attrelid and d.adnum operator(pg_catalog.=) a.attnum order by n.nspname, c.relname, attnum";
  }

  private int personOid() {
    return oidOf("Person");
  }

  private int oidOf(final String name) {
    final List<Object> oids = column(resolve("select oid from pg_class where relname = '" + name + "'"), "oid");
    assertThat(oids).hasSize(1);
    return (Integer) oids.get(0);
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
