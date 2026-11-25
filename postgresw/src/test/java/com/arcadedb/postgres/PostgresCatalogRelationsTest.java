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
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.jdbc.PgDatabaseMetaData;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Which catalog relations the emulated PostgreSQL catalog answers, how it answers the exact statements psql, psqlodbc and pgjdbc
 * send over them, which relations it refuses or answers with no row, and how joins between relations bind their rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresCatalogRelationsTest {

  /**
   * Regression test for issue #10228: the catalog queries psql 17 ({@code \dt}, {@code \d "Person"}) and psqlodbc
   * ({@code SQLColumns}, {@code SQLTables(SQL_ALL_TABLE_TYPES)}) send came back empty, unfiltered or as a syntax
   * error. Every statement below is the exact text those clients sent to a PostgreSQL 17 server, captured with
   * {@code log_statement=all}, and every expected answer is what PostgreSQL 17 answered for the same schema (minus
   * its own system relations, which this catalog does not have).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PsqlOdbcCatalogShapes {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogRelationsTest-PsqlOdbcCatalogShapes";
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

    // Regression: #10228
    @Test
    void aParenthesisedJoinIsReadAsTheJoinItWraps() {
      final PostgresCatalog.Answer answer = resolve(
          "select 1 from (pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace)");

      assertThat(answer.rows).hasSize(3);
    }

    // Regression: #10228
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

    // Regression: #10228
    /** The same with psqlodbc's exact-match spelling, {@code OPERATOR(pg_catalog.=)}. */
    @Test
    void odbcColumnsWithTheOperatorSpellingOfEquality() {
      final PostgresCatalog.Answer answer = resolve(odbcColumns(
          " and c.relname operator(pg_catalog.=) 'Person' and n.nspname operator(pg_catalog.=) '" + database.getName() + "'"));

      assertThat(column(answer, "attname")).containsExactly("id", "name");
    }

    // Regression: #10228
    /** {@code SQLColumns()} with no filter: every column of every type, and nothing for a type with no properties. */
    @Test
    void odbcColumnsOfEveryTable() {
      final PostgresCatalog.Answer answer = resolve(odbcColumns(""));

      assertThat(column(answer, "attname")).containsExactly("id", "name");
    }

    // Regression: #10228
    @Test
    void odbcColumnsOfATableThatDoesNotExistAreNone() {
      final PostgresCatalog.Answer answer = resolve(odbcColumns(" and c.relname like 'Nope'"));

      assertThat(answer.rows).isEmpty();
      assertThat(answer.columns).hasSize(18);
    }

    // ---------------------------------------------------------------- case 2: a join to pg_am

    // Regression: #10228
    @Test
    void aLeftJoinToPgAmKeepsEveryTable() {
      final PostgresCatalog.Answer answer = resolve(
          "select c.relname, am.amname from pg_catalog.pg_class c left join pg_catalog.pg_am am on am.oid = c.relam");

      assertThat(column(answer, "relname")).containsExactly("City", "Note", "Person");
      assertThat(column(answer, "amname")).containsExactly("heap", "heap", "heap");
    }

    // Regression: #10228
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

    // Regression: #10228
    @Test
    void theOperatorSpellingOfEqualityFilters() {
      assertThat(resolve("select relname from pg_catalog.pg_class where relname operator(pg_catalog.=) 'zzz'").rows).isEmpty();
      assertThat(column(resolve("select relname from pg_catalog.pg_class where relname OPERATOR(pg_catalog.=) 'Note'"),
          "relname")).containsExactly("Note");
      assertThat(column(resolve("select relname from pg_class where relname operator(<>) 'Note'"), "relname"))
          .containsExactly("City", "Person");
    }

    // Regression: #10228
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

    // Regression: #10228
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

    // Regression: #10228
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

    // Regression: #10228
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

    // Regression: #10228
    @Test
    void aUnionOfLiteralsIsAnswered() {
      final PostgresCatalog.Answer answer = resolve("select 1 union select 2");

      assertThat(answer.rows).hasSize(2);
      assertThat(column(answer, "?column?")).containsExactly(1L, 2L);
    }

    // Regression: #10228
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

    // Regression: #10228
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

    // Regression: #10228
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

    // Regression: #10228
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

    // Regression: #10228
    /** A union of a catalog relation and a literal arm is answered too. */
    @Test
    void aUnionMixingACatalogRelationAndLiterals() {
      final PostgresCatalog.Answer answer = resolve(
          "select relname from pg_class where relname = 'Note' union all select 'extra' order by 1");

      assertThat(column(answer, "relname")).containsExactly("Note", "extra");
    }

    // Regression: #10228
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

    // Regression: #10228
    /** Numbers compare by value when a set operation removes duplicates, as in SQL: 1 and 1.0 are one row. */
    @Test
    void setOperationsCompareNumbersByValue() {
      assertThat(resolve("select 1 as x union select 1.0").rows).hasSize(1);
      assertThat(resolve("select 1 as x union select 1.5").rows).hasSize(2);
    }

    // Regression: #10228
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

    // Regression: #10228
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

    // Regression: #10228
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

    // Regression: #10228
    @Test
    void groupByOverACatalogRelation() {
      final PostgresCatalog.Answer answer = resolve(
          "select c.relname, count(*) as columns from pg_class c join pg_attribute a on a.attrelid = c.oid group by c.relname");

      assertThat(column(answer, "relname")).containsExactly("Person");
      assertThat(column(answer, "columns")).containsExactly(2L);
    }

    // ---------------------------------------------------------------- the join semantics the fixes rest on

    // Regression: #10228
    /** A LEFT JOIN whose condition does not hold reads NULL from the joined relation, and keeps the row. */
    @Test
    void aLeftJoinThatDoesNotMatchReadsNull() {
      final PostgresCatalog.Answer answer = resolve(
          "select c.relname, n.nspname from pg_class c left join pg_namespace n on n.oid = c.relnamespace and n.nspname = 'other'");

      assertThat(column(answer, "relname")).containsExactly("City", "Note", "Person");
      assertThat(column(answer, "nspname")).containsExactly(null, null, null);
    }

    // Regression: #10228
    /** An INNER JOIN's condition is a filter, and a condition that does not hold removes the row. */
    @Test
    void anInnerJoinConditionFilters() {
      final PostgresCatalog.Answer answer = resolve(
          "select c.relname from pg_class c inner join pg_namespace n on n.oid = c.relnamespace and c.relname = 'City'");

      assertThat(column(answer, "relname")).containsExactly("City");
    }

    // Regression: #10228
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

    // Regression: #10228
    /** A scalar sub-select answering more than one row is an error in PostgreSQL, so it is not answered. */
    @Test
    void aScalarSubqueryWithMoreThanOneRowIsDeclined() {
      assertThat(PostgresCatalog.resolve("select (select relname from pg_class) from pg_namespace", database, USER))
          .isSameAs(PostgresCatalog.DECLINED);
    }

    // Regression: #10228
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

    // Regression: #10228
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

    // Regression: #10228
    /** COLLATE with a quoted or a schema-qualified quoted name is dropped whole, not half. */
    @Test
    void collateWithAQuotedNameIsDroppedWhole() {
      assertThat(column(resolve("select relname from pg_class where relname = 'Note' COLLATE \"C\""), "relname"))
          .containsExactly("Note");
      assertThat(column(resolve("select relname from pg_class where relname = 'Note' COLLATE pg_catalog.\"default\" "
          + "order by relname collate \"C\""), "relname")).containsExactly("Note");
    }

    // Regression: #10228
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

    // Regression: #10228
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

    // Regression: #10228
    /** A set operation naming no catalog relation is a SELECT: an INSERT that only contains the word is not one. */
    @Test
    void anInsertContainingASetOperationWordIsNotTokenized() {
      assertThat(PostgresCatalog.resolve("insert into Person set name = 'select 1 union select 2'", database, USER)).isNull();
    }

    // Regression: #10228
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

    // Regression: #10228
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

    // Regression: #10228
    /** A pattern Java cannot compile is not an answer: kept by the permissive filter, declined over pg_type. */
    @Test
    void anInvalidRegularExpressionIsUnknown() {
      assertThat(column(resolve("select relname from pg_class where relname ~ '(['"), "relname"))
          .containsExactly("City", "Note", "Person");
      assertThat(PostgresCatalog.resolve("select typname from pg_type where typname ~ '(['", database, USER))
          .isSameAs(PostgresCatalog.DECLINED);
    }

    // Regression: #10228
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

    // Regression: #10228
    /**
     * A second alias read in an inner join's condition reads a row of its own (issue #10495), as it does in PostgreSQL: every
     * relation paired with the one named Person.
     */
    @Test
    void aSecondAliasReadOnlyInAnInnerJoinConditionReadsItsOwnRow() {
      assertThat(column(resolve("select c.relname from pg_class c join pg_class p on p.relname = 'Person'"), "relname"))
          .containsExactly("City", "Note", "Person");
      assertThat(column(resolve("select c.relname from pg_class c join pg_class p on p.relname = 'Nobody'"), "relname")).isEmpty();
    }

    // Regression: #10228
    /** OFFSET and FETCH after a set operation's ORDER BY are outside the slice: never folded into the sort key. */
    @Test
    void anOffsetAfterASetOperationIsNotAnswered() {
      assertThat(PostgresCatalog.resolve("select 1 as x union select 2 order by 1 offset 1", database, USER)).isNull();
      assertThat(PostgresCatalog.resolve("select relname from pg_class union select 'x' order by 1 fetch first 1 rows only",
          database, USER)).isSameAs(PostgresCatalog.DECLINED);
    }

    // Regression: #10228
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

    // Regression: #10228
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
      // node, and the statement is still refused within the bound - the per-match budget cuts each match, and the regex
      // budget of the statement what all of them read (PostgresCatalogStatementPatternWorkTest pins the matches that stay
      // under the per-match budget).
      final StallAwareStopwatch correlated = StallAwareStopwatch.start();
      assertThatThrownBy(() -> PostgresCatalog.resolve("select c.relname from pg_class c where exists (select 1 from "
          + "pg_class d where d.oid = c.oid and d.relname ~ '^(.*)(.*)(.*)(.*)(.*)Y$')", database, USER))
          .isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
      correlated.assertGaveUpWithin(15_000, "the budget of the statement from a fresh regex budget per outer row");
    }

    // Regression: #10228
    /** string_agg's delimiter is the one of the row whose value follows it, as in PostgreSQL. */
    @Test
    void stringAggReadsTheDelimiterOfEachRow() {
      assertThat(column(resolve("select string_agg(relname, relname) as s from pg_class"), "s"))
          .containsExactly("CityNoteNotePersonPerson");
      assertThat(column(resolve("select string_agg(relname, ', ') as s from pg_class"), "s"))
          .containsExactly("City, Note, Person");
    }

    // Regression: #10228
    /**
     * A cross join of a relation with itself is the product, as in PostgreSQL (issue #10495): each alias has the rows of the relation
     * of its own. The product is bounded by the statement's work budget, so a join of many aliases is refused rather than built.
     */
    @Test
    void aSelfCrossJoinIsTheProductAndIsBoundedByTheWorkBudget() {
      final int attributes = resolve("select attname from pg_attribute").rows.size();
      assertThat(attributes).isGreaterThan(1);

      assertThat(resolve("select a.attname, b.attname from pg_attribute a, pg_attribute b").rows).hasSize(attributes * attributes);
      assertThat(resolve("select a.attname, b.attname from pg_attribute a cross join pg_attribute b").rows)
          .hasSize(attributes * attributes);

      // every alias is read by the projection: an alias nothing reads is not a row of the product. The relation has few rows, so it
      // takes many aliases for the product to pass the budget
      final StringBuilder projection = new StringBuilder("a0.attname");
      final StringBuilder from = new StringBuilder("pg_attribute a0");
      for (int i = 1; i < 30; i++) {
        projection.append(", a").append(i).append(".attname");
        from.append(", pg_attribute a").append(i);
      }
      assertThatThrownBy(() -> PostgresCatalog.resolve("select " + projection + " from " + from, database, USER)).isInstanceOf(
          PostgresCatalog.WorkLimitExceeded.class);
    }

    // Regression: #10228
    @Test
    void anOuterJoinToAParenthesisedJoinOrAnAliasedOneIsDeclined() {
      assertThat(PostgresCatalog.resolve("select c.relname from pg_class c left join (pg_attribute a join pg_type t "
          + "on t.oid = a.atttypid) on a.attrelid = c.oid", database, USER)).isSameAs(PostgresCatalog.DECLINED);
      assertThat(PostgresCatalog.resolve("select 1 from (pg_class c join pg_namespace n on n.oid = c.relnamespace) j",
          database, USER)).isSameAs(PostgresCatalog.DECLINED);
    }

    // Regression: #10228
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
  @Nested
  class UnmodelledRelations {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogRelationsTest-UnmodelledRelations";
    private static final String USER          = "root";

    /** Relations PostgreSQL has and this catalog does not model: some are empty in ArcadeDB's world, some are not. */
    private static final List<String> UNMODELLED = List.of("pg_catalog.pg_extension", "pg_extension", "pg_locks", "pg_stat_database",
        "pg_stat_activity", "pg_stat_user_tables", "pg_language", "pg_opclass", "information_schema.referential_constraints",
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

    // Regression: #10371
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

    // Regression: #10371
    /** A relation PostgreSQL really has and this catalog does not model is declined, so the executor can refuse it. */
    @Test
    void anUnmodelledRelationIsDeclinedAndNeverAnsweredEmpty() {
      for (final String relation : UNMODELLED) {
        final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select count(*) from " + relation, database, USER);
        assertThat(answer).as(relation).isSameAs(PostgresCatalog.DECLINED);
        assertThat(answer.rows).as("a refusal carries no rows to send: %s", relation).isNull();
      }
    }

    // Regression: #10371
    @Test
    void aRefusalNamesTheRelationItCouldNotModel() {
      assertThat(PostgresCatalog.refusalOf("select count(*) from pg_catalog.pg_extension")).isInstanceOf(
          PostgresCatalog.UnsupportedCatalogQuery.class).hasMessageContaining("\"pg_extension\"");
      assertThat(PostgresCatalog.refusalOf("select * from information_schema.routines where 1 = 1"))
          .hasMessageContaining("\"information_schema.routines\"");
      // a function is not a relation: the one named is the relation it is read from
      assertThat(PostgresCatalog.refusalOf("select pg_catalog.pg_get_userbyid(e.extowner) from pg_catalog.pg_extension e"))
          .hasMessageContaining("\"pg_extension\"").hasMessageNotContaining("pg_get_userbyid");
      // a shape with no unmodelled relation in it still says it is refused, and why
      assertThat(PostgresCatalog.refusalOf("select relname from pg_class a, pg_class b, pg_class c where a.oid = b.oid"))
          .hasMessageContaining("shape");
      assertThat(PostgresCatalog.UnsupportedCatalogQuery.SQLSTATE).as("feature_not_supported").isEqualTo("0A000");
    }

    // Regression: #10371
    /** A column reference is not a relation: {@code pg_type.oid} must not be named as the relation that was refused. */
    @Test
    void aColumnReferenceIsNotNamedAsTheRefusedRelation() {
      assertThat(PostgresCatalog.refusalOf("select pg_type.oid from pg_catalog.pg_extension, pg_catalog.pg_type"))
          .hasMessageContaining("\"pg_extension\"").hasMessageNotContaining("pg_type.oid");
      assertThat(PostgresCatalog.refusalOf("select pg_type.oid from pg_catalog.pg_type, pg_class a, pg_class b, pg_class c"))
          .hasMessageContaining("shape");
    }

    // Regression: #10371
    /**
     * The lookups a driver sends to name a type it does not know have no exemption from the refusal (issue #10495): they are answered
     * from {@link PostgresType} (see {@code PostgresTypeCatalogTest.TypeLookup}), and a type query this catalog cannot read is refused like any
     * other, whatever relations it names.
     */
    @Test
    void aTypeQueryThatCannotBeReadIsRefusedLikeAnyOtherCatalogQuery() {
      // another relation the catalog does not model
      assertThat(PostgresCatalog.resolve("select * from pg_type t, pg_extension e where e.extnamespace = t.typnamespace", database,
          USER))
          .isSameAs(PostgresCatalog.DECLINED);
      // a filter it cannot evaluate, which over the fixed set of types would otherwise select every one of them
      assertThat(PostgresCatalog.resolve("select t.oid from pg_type t where t.typname = lookup_something(t.oid)", database, USER))
          .isSameAs(PostgresCatalog.DECLINED);
      // a lookup of a table function the evaluator does not know
      assertThat(PostgresCatalog.resolve("select t.oid from pg_type t, generate_subscripts(current_schemas(true), 1) s", database,
          USER)).isSameAs(PostgresCatalog.DECLINED);
    }

    // Regression: #10371
    /** The control: a modelled relation with no matching row still answers PostgreSQL's one row holding 0. */
    @Test
    void aModelledRelationStillAnswersItsZero() {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select count(*) from pg_catalog.pg_class where relname = 'Nope'",
          database, USER);

      assertThat(answer.rows).hasSize(1);
      assertThat(((Number) answer.rows.get(0).values().iterator().next()).longValue()).isZero();
    }

    // Regression: #10371
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

    // Regression: #10371
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

  /**
   * Issue #10591: a catalog relation ArcadeDB can never hold a row in was refused with 0A000 (since #10371) instead of answered with
   * no rows, so psql's {@code \df} and {@code \du} and pgjdbc's {@code getImportedKeys()} and {@code getFunctions()} failed where
   * "there are none" is the honest answer. {@code pg_settings} holds a row per parameter in every PostgreSQL, so it is modelled
   * with the values {@code SHOW} answers, and a relation whose absence would be a lie is still refused.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NoRowRelations {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogRelationsTest-NoRowRelations";
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

    // Regression: #10591
    @Test
    void aRelationWithNoEquivalentAnswersNoRowAndItsCountZero() {
      for (final String relation : NO_ROW_RELATIONS) {
        assertThat(resolve("select * from pg_catalog." + relation).rows).as(relation).isEmpty();

        final PostgresCatalog.Answer count = resolve("select count(*) from pg_catalog." + relation);
        assertThat(count.rows).as(relation).hasSize(1);
        assertThat(((Number) count.rows.get(0).values().iterator().next()).longValue()).as(relation).isZero();
      }
    }

    // Regression: #10591
    @Test
    void aNoRowRelationAnswersItsColumnsByName() {
      final PostgresCatalog.Answer answer = resolve("select proname, prokind, pronamespace from pg_catalog.pg_proc");
      assertThat(answer.rows).isEmpty();
      assertThat(answer.columns.keySet()).containsExactly("proname", "prokind", "pronamespace");
    }

    // Regression: #10591
    @Test
    void psqlDescribeFunctionsAnswersNoFunction() {
      final PostgresCatalog.Answer answer = resolve(PSQL_DF);
      assertThat(answer.rows).isEmpty();
      assertThat(answer.columns.keySet()).containsExactly("Schema", "Name", "Result data type", "Argument data types", "Type");
    }

    // Regression: #10591
    @Test
    void psqlDescribeUsersListsTheUserWithNoMembership() {
      final PostgresCatalog.Answer answer = resolve(PSQL_DU);
      assertThat(answer.rows).hasSize(1);
      assertThat(answer.rows.get(0).get("rolname")).isEqualTo(USER);
      assertThat((List<?>) answer.rows.get(0).get("memberof")).isEmpty();
    }

    // Regression: #10591
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

    // Regression: #10591
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

    // Regression: #10591
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

    // Regression: #10591
    @Test
    void serverVersionNumReadsTheLeadingDigitsOfTheVersion() {
      assertThat(PostgresSessionSettings.serverVersionNum("12.0")).isEqualTo("120000");
      assertThat(PostgresSessionSettings.serverVersionNum("16.4 (ArcadeDB)")).isEqualTo("160004");
      assertThat(PostgresSessionSettings.serverVersionNum("17")).isEqualTo("170000");
      assertThat(PostgresSessionSettings.serverVersionNum("devel")).isEqualTo("0");
      assertThat(new PostgresSessionSettings().show("server_version_num")).isEqualTo(
          PostgresSessionSettings.serverVersionNum(PostgresNetworkExecutor.PG_SERVER_VERSION));
    }

    // Regression: #10591
    @Test
    void pgTablespaceListsTheTwoEveryPostgresqlHas() {
      final PostgresCatalog.Answer answer = resolve(
          "SELECT spcname AS \"Name\", pg_catalog.pg_get_userbyid(spcowner) AS \"Owner\", pg_catalog.pg_tablespace_location(oid) "
              + "AS \"Location\" FROM pg_catalog.pg_tablespace ORDER BY 1");
      assertThat(answer.rows).extracting(row -> row.get("Name")).containsExactly("pg_default", "pg_global");
      assertThat(answer.rows).extracting(row -> row.get("Location")).containsExactly("", "");
    }

    // Regression: #10591
    /** The refusal of #10371 stands for a relation that holds rows in PostgreSQL and is not modelled here. */
    @Test
    void aRelationWhoseAbsenceWouldBeALieIsStillDeclined() {
      for (final String relation : List.of("pg_stat_activity", "pg_extension", "pg_locks", "information_schema.routines"))
        assertThat(PostgresCatalog.resolve("select count(*) from " + relation, database, USER)).as(relation)
            .isSameAs(PostgresCatalog.DECLINED);
    }

    // Regression: #10591
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

    /*
     * Issue #10960: the catalog relations psql's {@code \dy}, {@code \dew}, {@code \des}, {@code \deu}, {@code \dRs}, {@code \drds},
     * {@code \lo_list} and {@code \dconfig+} read hold no row in a stock PostgreSQL either, and describe objects ArcadeDB has no
     * equivalent of, so they are answered with no rows instead of the 0A000 of an unmodelled relation (#10842).
     *
     * @author Luca Garulli (l.garulli@arcadedata.com)
     */
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

    // Regression: #10960
    @Test
    void eachRelationAnswersNoRowAndItsCountZero() {
      for (final String relation : RELATIONS) {
        assertThat(PostgresCatalog.resolve("select * from pg_catalog." + relation, database, USER)).as(relation)
            .isNotSameAs(PostgresCatalog.DECLINED);
        assertThat(resolveAnswered("select * from pg_catalog." + relation).rows).as(relation).isEmpty();
        assertThat(resolveAnswered("select * from " + relation).rows).as(relation + " unqualified").isEmpty();

        final PostgresCatalog.Answer count = resolveAnswered("select count(*) from pg_catalog." + relation);
        assertThat(count.rows).as(relation).hasSize(1);
        assertThat(((Number) count.rows.get(0).values().iterator().next()).longValue()).as(relation).isZero();
      }
    }

    // Regression: #10960
    @Test
    void aColumnIsAnsweredByName() {
      final PostgresCatalog.Answer answer = resolveAnswered("select srvname, srvowner, srvfdw from pg_catalog.pg_foreign_server");
      assertThat(answer.rows).isEmpty();
      assertThat(answer.columns.keySet()).containsExactly("srvname", "srvowner", "srvfdw");
    }

    // Regression: #10960
    @Test
    void psqlCommandsAnswerNoRow() {
      assertThat(resolveAnswered(PSQL_DY).columns.keySet()).containsExactly("Name", "Event", "Owner", "Enabled", "Function", "Description");
      assertThat(resolveAnswered(PSQL_DY).rows).isEmpty();
      assertThat(resolveAnswered(PSQL_DEW).columns.keySet()).containsExactly("Name", "Owner", "Handler", "Validator");
      assertThat(resolveAnswered(PSQL_DEW).rows).isEmpty();
      assertThat(resolveAnswered(PSQL_DES).columns.keySet()).containsExactly("Name", "Owner", "Foreign-data wrapper");
      assertThat(resolveAnswered(PSQL_DES).rows).isEmpty();
      assertThat(resolveAnswered(PSQL_DEU).columns.keySet()).containsExactly("Server", "User name");
      assertThat(resolveAnswered(PSQL_DEU).rows).isEmpty();
      assertThat(resolveAnswered(PSQL_DRS).columns.keySet()).containsExactly("Name", "Owner", "Enabled", "Publication");
      assertThat(resolveAnswered(PSQL_DRS).rows).isEmpty();
      assertThat(resolveAnswered(PSQL_DRDS).columns.keySet()).containsExactly("Role", "Database", "Settings");
      assertThat(resolveAnswered(PSQL_DRDS).rows).isEmpty();
      assertThat(resolveAnswered(PSQL_LO_LIST).columns.keySet()).containsExactly("ID", "Owner", "Description");
      assertThat(resolveAnswered(PSQL_LO_LIST).rows).isEmpty();
    }

    // Regression: #10960
    /** The columns of an empty answer are announced with the type PostgreSQL gives them, not as text. */
    @Test
    void anEmptyAnswerAnnouncesTheTypesOfItsOidAndFlagColumns() {
      assertThat(PostgresCatalog.declaredType("pg_subscription", "subenabled")).isEqualTo(PostgresType.BOOLEAN);
      assertThat(PostgresCatalog.declaredType("pg_subscription", "subowner")).isEqualTo(PostgresType.INTEGER);
      assertThat(PostgresCatalog.declaredType("pg_foreign_server", "srvfdw")).isEqualTo(PostgresType.INTEGER);
      assertThat(PostgresCatalog.declaredType("pg_foreign_server", "srvname")).isEqualTo(PostgresType.VARCHAR);
    }

    // Regression: #10960
    /**
     * A relation PostgreSQL holds rows in that this catalog does not model is still refused: the change is not "answer none". Every
     * one of them, not a sample of three that does not grow with the no-row set (issue #11023).
     */
    @Test
    void aRelationThatHoldsRowsInEveryPostgresIsStillRefused() {
      for (final String relation : RelationPolicy.HOLDS_ROWS_IN_EVERY_POSTGRES.keySet())
        if (!PostgresCatalog.modelledRelations().contains(relation))
          assertThat(PostgresCatalog.resolve("select * from " + RelationPolicy.qualified(relation), database, USER)).as(relation)
              .isSameAs(PostgresCatalog.DECLINED);
      // and none of the relations of this issue is one of them
      assertThat(RELATIONS).doesNotContainAnyElementsOf(RelationPolicy.HOLDS_ROWS_IN_EVERY_POSTGRES.keySet());
    }

    private PostgresCatalog.Answer resolveAnswered(final String query) {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
      assertThat(answer).as(query).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
      return answer;
    }

    private PostgresCatalog.Answer resolve(final String query) {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
      assertThat(answer).as("query was not recognised as a catalog query: %s", query).isNotNull();
      assertThat(answer).as("query was declined: %s", query).isNotSameAs(PostgresCatalog.DECLINED);
      return answer;
    }
  }

  /**
   * Issue #10842: the policy for a catalog relation this server does not model, and the {@code pg_depend} rows of the indexes.
   * <ol>
   * <li>An unmodelled relation is refused by default, not answered with no rows: most of them hold rows in every PostgreSQL.</li>
   * <li>Every relation psql's {@code describe.c} and psqlodbc's {@code info.c} read is either modelled or refused on purpose, with
   * the reason written next to it, the way #10591 holds pgjdbc's {@code PgDatabaseMetaData} to the catalog. Neither source is on the
   * test classpath, so their relations are checked in here: a client release that reads a new relation is added to the list.</li>
   * <li>{@code pg_depend} lists the dependencies {@code index_create} records for each index, on its table's columns or on the
   * unique constraint it backs, instead of no row at all.</li>
   * </ol>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RelationPolicy {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogRelationsTest-RelationPolicy";
    private static final String USER          = "root";

    /** {@code pg_class} and {@code pg_constraint} as relations, the values pg_depend's classid and refclassid hold. */
    private static final int PG_CLASS      = 1259;
    private static final int PG_CONSTRAINT = 2606;
    /** pg_namespace and pg_type, the other catalogs pg_depend names since issue #10961. */
    private static final int PG_NAMESPACE  = 2615;
    private static final int PG_TYPE       = 1247;

    /**
     * The catalog relations psql 17's {@code src/bin/psql/describe.c} reads (REL_17_STABLE), extracted from its SQL text: every
     * {@code pg_*} name it puts in a FROM or JOIN, without its functions ({@code pg_get_*}, {@code *_is_visible}, ...).
     */
    static final List<String> PSQL_DESCRIBE_RELATIONS = List.of("pg_am", "pg_amop", "pg_amproc", "pg_attrdef", "pg_attribute",
        "pg_auth_members", "pg_cast", "pg_class", "pg_collation", "pg_constraint", "pg_conversion", "pg_database",
        "pg_db_role_setting", "pg_default_acl", "pg_depend", "pg_description", "pg_enum", "pg_event_trigger", "pg_extension",
        "pg_foreign_data_wrapper", "pg_foreign_server", "pg_foreign_table", "pg_index", "pg_inherits", "pg_language",
        "pg_largeobject_metadata", "pg_namespace", "pg_opclass", "pg_operator", "pg_opfamily", "pg_parameter_acl", "pg_policy",
        "pg_proc", "pg_publication", "pg_publication_namespace", "pg_publication_rel", "pg_rewrite", "pg_roles", "pg_sequence",
        "pg_settings", "pg_statistic_ext", "pg_subscription", "pg_tablespace", "pg_trigger", "pg_ts_config", "pg_ts_config_map",
        "pg_ts_dict", "pg_ts_parser", "pg_ts_template", "pg_type", "pg_user_mappings");

    /** The catalog relations psqlodbc's {@code info.c} reads (main branch), extracted the same way. */
    static final List<String> PSQLODBC_INFO_RELATIONS = List.of("pg_am", "pg_attrdef", "pg_attribute", "pg_class", "pg_constraint",
        "pg_group", "pg_index", "pg_namespace", "pg_proc", "pg_trigger", "pg_type", "pg_user", "information_schema.column_privileges");

    /**
     * The relations those clients read that this catalog refuses on purpose, each with why. Every one holds rows in a stock
     * PostgreSQL, so every one is in {@link #HOLDS_ROWS_IN_EVERY_POSTGRES} as well (issue #11205): answering it with no rows would
     * claim the database has none, which is the #10371 failure. (What a stock PostgreSQL holds no row of, and ArcadeDB has no
     * equivalent of, is modelled with no rows instead: issue #10960.)
     */
    static final Map<String, String> REFUSED_ON_PURPOSE = Map.ofEntries(//
        Map.entry("pg_amop", "the operators of the built-in operator families (psql \\dAo)"),
        Map.entry("pg_amproc", "the support functions of the built-in operator families (psql \\dAp)"),
        Map.entry("pg_cast", "the built-in casts (psql \\dC)"),
        Map.entry("pg_conversion", "the built-in encoding conversions (psql \\dc)"),
        Map.entry("pg_extension", "plpgsql is installed in every PostgreSQL (psql \\dx)"),
        Map.entry("pg_language", "internal, c, sql and plpgsql exist in every PostgreSQL (psql \\dL)"),
        Map.entry("pg_opclass", "the built-in operator classes (psql \\dAc)"),
        Map.entry("pg_operator", "the built-in operators (psql \\do)"),
        Map.entry("pg_opfamily", "the built-in operator families (psql \\dAf)"),
        Map.entry("pg_ts_config", "the built-in text-search configurations (psql \\dF)"),
        Map.entry("pg_ts_config_map", "the mappings of the built-in text-search configurations (psql \\dF+)"),
        Map.entry("pg_ts_dict", "the built-in text-search dictionaries (psql \\dFd)"),
        Map.entry("pg_ts_parser", "the built-in text-search parser (psql \\dFp)"),
        Map.entry("pg_ts_template", "the built-in text-search templates (psql \\dFt)"),
        Map.entry("pg_group", "the predefined roles that cannot log in exist in every PostgreSQL (psqlodbc SQLTablePrivileges)"),
        Map.entry("information_schema.column_privileges", "a row per column privilege the user holds (psqlodbc SQLColumnPrivileges)"));

    /**
     * Issue #11023: the relations that hold rows in EVERY PostgreSQL, a fresh {@code initdb} of PostgreSQL 17, with what the rows
     * are. A fact about PostgreSQL, not about this catalog, so it does not move when the catalog does: the bootstrap data of
     * {@code src/include/catalog/*.dat} and what {@code initdb} creates after it (the plpgsql extension, the system views and their
     * rules, the comments). Answering one of these with no rows claims a PostgreSQL that does not exist, which is the #10371 lie,
     * and {@link #NO_ROWS_ALTHOUGH_POSTGRES_HOLDS_ROWS} is the only way past it.
     */
    static final Map<String, String> HOLDS_ROWS_IN_EVERY_POSTGRES = Map.ofEntries(//
        Map.entry("pg_aggregate", "the built-in aggregates (pg_aggregate.dat)"),
        Map.entry("pg_am", "the built-in access methods: heap, btree, hash, gist, gin, spgist, brin (pg_am.dat)"),
        Map.entry("pg_amop", "the operators of the built-in operator families (pg_amop.dat)"),
        Map.entry("pg_amproc", "the support functions of the built-in operator families (pg_amproc.dat)"),
        Map.entry("pg_attribute", "the columns of the system catalogs and views"),
        Map.entry("pg_authid", "the bootstrap superuser and the predefined roles (pg_authid.dat)"),
        Map.entry("pg_auth_members", "the memberships of the predefined roles: pg_monitor in pg_read_all_settings, ..."),
        Map.entry("pg_cast", "the built-in casts (pg_cast.dat)"),
        Map.entry("pg_class", "the system catalogs, their indexes and the system views"),
        Map.entry("pg_collation", "default, C, POSIX, ucs_basic and the collations initdb imports (pg_collation.dat)"),
        Map.entry("pg_constraint", "the CHECK constraints of the information_schema domains (cardinal_number, ...)"),
        Map.entry("pg_conversion", "the built-in encoding conversions (pg_conversion.dat)"),
        Map.entry("pg_database", "postgres, template0 and template1 (pg_database.dat)"),
        Map.entry("pg_depend", "the dependencies of the system views and of the information_schema"),
        Map.entry("pg_description", "the comments on the built-in objects (postgres.description)"),
        Map.entry("pg_extension", "plpgsql, which initdb installs"),
        Map.entry("pg_group", "the predefined roles that cannot log in"),
        Map.entry("pg_index", "the indexes of the system catalogs"),
        Map.entry("pg_init_privs", "the initial privileges of the system objects, which initdb records"),
        Map.entry("pg_language", "internal, c, sql (pg_language.dat) and plpgsql"),
        Map.entry("pg_namespace", "pg_catalog, pg_toast, public (pg_namespace.dat) and information_schema"),
        Map.entry("pg_opclass", "the built-in operator classes (pg_opclass.dat)"),
        Map.entry("pg_operator", "the built-in operators (pg_operator.dat)"),
        Map.entry("pg_opfamily", "the built-in operator families (pg_opfamily.dat)"),
        Map.entry("pg_proc", "the built-in functions (pg_proc.dat)"),
        Map.entry("pg_range", "the built-in range types: int4range, int8range, numrange, tsrange, tstzrange, daterange (pg_range.dat)"),
        Map.entry("pg_rewrite", "the _RETURN rules of the system views"),
        Map.entry("pg_roles", "the bootstrap superuser and the predefined roles"),
        Map.entry("pg_settings", "a row per configuration parameter"),
        Map.entry("pg_shdescription", "the comments on postgres, template0 and template1"),
        Map.entry("pg_tablespace", "pg_default and pg_global (pg_tablespace.dat)"),
        Map.entry("pg_ts_config", "the built-in text-search configurations (pg_ts_config.dat)"),
        Map.entry("pg_ts_config_map", "the mappings of the built-in text-search configurations (pg_ts_config_map.dat)"),
        Map.entry("pg_ts_dict", "the built-in text-search dictionaries (pg_ts_dict.dat)"),
        Map.entry("pg_ts_parser", "the built-in text-search parser (pg_ts_parser.dat)"),
        Map.entry("pg_ts_template", "the built-in text-search templates (pg_ts_template.dat)"),
        Map.entry("pg_type", "the built-in types (pg_type.dat) and the row types of the system catalogs"),
        Map.entry("pg_user", "the bootstrap superuser"),
        Map.entry("information_schema.column_privileges", "a row per column privilege on the system catalogs and views, which "
            + "PUBLIC may SELECT and the bootstrap superuser owns"));

    /**
     * Issue #11205: the relations a fresh {@code initdb} of PostgreSQL 17 holds no row in, the other half of the same fact. A relation
     * that enters the no-row set has to be in one of the two lists, so the judgement that PostgreSQL holds no row there is written
     * down by name too, and cannot be skipped by keeping the relation out of {@link #HOLDS_ROWS_IN_EVERY_POSTGRES}: before, a
     * relation in neither list entered the no-row set with every test green.
     */
    static final Map<String, String> HOLDS_NO_ROWS_IN_STOCK_POSTGRES = Map.ofEntries(//
        Map.entry("pg_policy", "no table of a fresh database has a row-level security policy"),
        Map.entry("pg_statistic_ext", "no CREATE STATISTICS has run"),
        Map.entry("pg_publication", "no publication is created by initdb"),
        Map.entry("pg_publication_rel", "there is no publication"),
        Map.entry("pg_publication_namespace", "there is no publication"),
        Map.entry("pg_enum", "no enum type is built in"),
        Map.entry("pg_trigger", "no system catalog has a trigger"),
        Map.entry("pg_sequence", "no system object is a sequence"),
        Map.entry("pg_default_acl", "no ALTER DEFAULT PRIVILEGES has run"),
        Map.entry("pg_foreign_table", "there is no foreign server"),
        Map.entry("pg_event_trigger", "no event trigger is created by initdb"),
        Map.entry("pg_foreign_data_wrapper", "no foreign-data wrapper is installed by initdb"),
        Map.entry("pg_foreign_server", "no foreign server is created by initdb"),
        Map.entry("pg_user_mappings", "there is no foreign server"),
        Map.entry("pg_subscription", "no subscription is created by initdb"),
        Map.entry("pg_db_role_setting", "no ALTER ROLE ... SET or ALTER DATABASE ... SET has run"),
        Map.entry("pg_largeobject_metadata", "no large object is created by initdb"),
        Map.entry("pg_parameter_acl", "no GRANT ... ON PARAMETER has run"));

    /**
     * Issue #11023: the relations answered with no rows although every PostgreSQL holds rows in them, each a decision of its own
     * (issue #10591): the rows PostgreSQL holds describe objects this catalog lists nowhere else, so no row here could name them and
     * no client sees a contradiction. The reason is the production one, {@code PostgresCatalog.noRowRelations()}, and it has to say
     * so. A relation that enters the no-row set is checked against {@link #HOLDS_ROWS_IN_EVERY_POSTGRES}: one that is in it fails
     * {@link #aNoRowRelationHoldsNoRowInAStockPostgresUnlessItIsADecision} until it is added here as well, by a reviewer who read
     * what PostgreSQL holds there.
     */
    static final Set<String> NO_ROWS_ALTHOUGH_POSTGRES_HOLDS_ROWS = Set.of("pg_proc", "pg_range", "pg_auth_members", "pg_shdescription");

    private Database database;

    @BeforeEach
    void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();

      database = factory.create();
      // a unique index over two columns, which backs a constraint; a plain index over one column; and an index whose key is a hidden
      // property, which PostgreSQL calls an expression
      database.transaction(() -> {
        final Schema schema = database.getSchema();
        final DocumentType account = schema.createDocumentType("Account");
        account.createProperty("code", Type.STRING);
        account.createProperty("region", Type.STRING);
        account.createProperty("secret", Type.INTEGER);
        account.createProperty("note", Type.STRING);
        schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Account", "code", "region");
        schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Account", "note");
        schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Account", "secret");
        schema.createDocumentType("Plain").createProperty("name", Type.STRING);
      });
      database.command("sql", "ALTER PROPERTY Account.secret HIDDEN true");
    }

    @AfterEach
    void dropDatabase() {
      if (database != null && database.isOpen())
        database.drop();
    }

    // ------------------------------------------------------------------------------------------------- question 1: the default

    // Regression: #10842
    /** The decision: a relation that is neither modelled nor known is refused, never answered "none". */
    @Test
    void anUnmodelledRelationIsRefusedByDefault() {
      for (final String relation : List.of("pg_stat_activity", "pg_locks", "pg_extension", "pg_language", "pg_opclass",
          "information_schema.routines", "pg_relation_added_by_a_future_release")) {
        assertThat(PostgresCatalog.resolve("select * from " + relation, database, USER)).as(relation).isSameAs(PostgresCatalog.DECLINED);
        assertThat(PostgresCatalog.refusalOf("select * from " + relation).getMessage()).as(relation).contains("\"" + relation + "\"");
      }
    }

    // ------------------------------------------------------------------------------------------------- question 2: the guards

    // Regression: #10842
    @Test
    void everyRelationPsqlDescribeReadsIsModelledOrRefusedOnPurpose() {
      assertModelledOrRefusedOnPurpose("psql describe.c", PSQL_DESCRIBE_RELATIONS);
    }

    // Regression: #10842
    @Test
    void everyRelationPsqlodbcReadsIsModelledOrRefusedOnPurpose() {
      assertModelledOrRefusedOnPurpose("psqlodbc info.c", PSQLODBC_INFO_RELATIONS);
    }

    // Regression: #10842
    /**
     * A relation the catalog later models leaves the refusal list in the same change, so the list never lies about the catalog. The
     * move is to model it WITH its rows: a move to the no-row set is a different claim, which
     * {@link #aNoRowRelationHoldsNoRowInAStockPostgresUnlessItIsADecision} holds to what PostgreSQL holds (issue #11023).
     */
    @Test
    void noRelationIsBothModelledAndRefusedOnPurpose() {
      final Set<String> both = new TreeSet<>(REFUSED_ON_PURPOSE.keySet());
      both.retainAll(PostgresCatalog.modelledRelations());
      assertThat(both).as("relations both modelled and refused on purpose: model the rows PostgreSQL holds and drop the refusal, "
          + "never answer one with no rows").isEmpty();

      final Set<String> read = new HashSet<>(PSQL_DESCRIBE_RELATIONS);
      read.addAll(PSQLODBC_INFO_RELATIONS);
      assertThat(read).as("an entry of the refusal list no client reads").containsAll(REFUSED_ON_PURPOSE.keySet());
    }

    // ------------------------------------------------------------------------------------------------- the no-row set (#11023)

    // Regression: #10842
    /** Every relation answered with no rows says why no row is the truth: the judgement is written where it is made. */
    @Test
    void everyNoRowRelationSaysWhyItHoldsNoRow() {
      assertThat(PostgresCatalog.noRowRelations()).as("the no-row set").isNotEmpty();
      for (final Map.Entry<String, String> entry : PostgresCatalog.noRowRelations().entrySet())
        assertThat(entry.getValue()).as("why %s holds no row", entry.getKey()).isNotBlank().hasSizeGreaterThan(30)
            .doesNotContainIgnoringCase("todo");
    }

    // Regression: #10842
    /**
     * The no-row set is open in production; this closes it against a fact that does not move with it. A relation every PostgreSQL
     * holds rows in is answered with none only as a decision named in {@link #NO_ROWS_ALTHOUGH_POSTGRES_HOLDS_ROWS}, so moving
     * {@code pg_amop} from refused to no-row (the #11023 mutation, every other test green) fails here.
     */
    @Test
    void aNoRowRelationHoldsNoRowInAStockPostgresUnlessItIsADecision() {
      final Set<String> holdRowsInPostgres = new TreeSet<>(PostgresCatalog.noRowRelations().keySet());
      holdRowsInPostgres.retainAll(HOLDS_ROWS_IN_EVERY_POSTGRES.keySet());
      assertThat(holdRowsInPostgres)
          .as("relations every PostgreSQL holds rows in, answered with none: refuse them (REFUSED_ON_PURPOSE) or model their rows, "
              + "unless the rows PostgreSQL holds describe objects no other relation of this catalog lists - then say so in "
              + "PostgresCatalog.NEVER_HAVE_ROWS and add the relation to NO_ROWS_ALTHOUGH_POSTGRES_HOLDS_ROWS")
          .containsExactlyInAnyOrderElementsOf(NO_ROWS_ALTHOUGH_POSTGRES_HOLDS_ROWS);
    }

    // Regression: #10842
    /**
     * Issue #11205: the no-row set is closed against both halves of the fact, not against one. Every relation answered with no rows
     * is in one of the two lists of what PostgreSQL holds, and in one only, so a relation that is in neither cannot enter the set
     * unexamined: its classification is written down first, and if PostgreSQL holds rows there the test above asks for the decision.
     */
    @Test
    void everyNoRowRelationIsClassifiedByWhatPostgresHolds() {
      final Set<String> both = new TreeSet<>(HOLDS_ROWS_IN_EVERY_POSTGRES.keySet());
      both.retainAll(HOLDS_NO_ROWS_IN_STOCK_POSTGRES.keySet());
      assertThat(both).as("relations listed as holding rows in every PostgreSQL AND as holding none").isEmpty();

      final Set<String> unclassified = new TreeSet<>(PostgresCatalog.noRowRelations().keySet());
      unclassified.removeAll(HOLDS_ROWS_IN_EVERY_POSTGRES.keySet());
      unclassified.removeAll(HOLDS_NO_ROWS_IN_STOCK_POSTGRES.keySet());
      assertThat(unclassified).as("relations answered with no rows whose rows in a stock PostgreSQL nobody looked at: add each to "
          + "HOLDS_ROWS_IN_EVERY_POSTGRES or to HOLDS_NO_ROWS_IN_STOCK_POSTGRES, by what a fresh initdb holds").isEmpty();

      // and the no-row list of what PostgreSQL holds lists only relations answered with no rows, so it does not drift from the set
      assertThat(PostgresCatalog.noRowRelations().keySet()).as("HOLDS_NO_ROWS_IN_STOCK_POSTGRES names a relation not answered with no rows")
          .containsAll(HOLDS_NO_ROWS_IN_STOCK_POSTGRES.keySet());
    }

    // Regression: #10842
    /**
     * Issue #11205: a relation is refused on purpose BECAUSE PostgreSQL holds rows in it, so the list of what PostgreSQL holds is the
     * backstop of the refusal list. Deleting a relation from {@link #REFUSED_ON_PURPOSE} to move it to the no-row set (the deletion
     * {@link #noRelationIsBothModelledAndRefusedOnPurpose} forces) then still leaves it here, and the no-row test fails on it.
     */
    @Test
    void everyRelationRefusedOnPurposeHoldsRowsInEveryPostgres() {
      assertThat(HOLDS_ROWS_IN_EVERY_POSTGRES.keySet()).as("relations refused on purpose that HOLDS_ROWS_IN_EVERY_POSTGRES does not list")
          .containsAll(REFUSED_ON_PURPOSE.keySet());
    }

    // Regression: #10842
    /** Every relation that holds rows in every PostgreSQL is modelled, or refused: never answered "none" by default. */
    @Test
    void aRelationThatHoldsRowsInEveryPostgresIsModelledOrRefused() {
      for (final String relation : HOLDS_ROWS_IN_EVERY_POSTGRES.keySet()) {
        if (PostgresCatalog.modelledRelations().contains(relation))
          continue; // its rows, or a decision the test above holds it to
        assertThat(PostgresCatalog.resolve("select * from " + qualified(relation), database, USER)).as(relation)
            .isSameAs(PostgresCatalog.DECLINED);
        assertThat(PostgresCatalog.resolve("select count(*) from " + relation, database, USER)).as(relation + " count")
            .isSameAs(PostgresCatalog.DECLINED);
      }
    }

    private void assertModelledOrRefusedOnPurpose(final String client, final List<String> relations) {
      final List<String> unaccounted = new ArrayList<>();
      for (final String relation : relations) {
        if (PostgresCatalog.modelledRelations().contains(relation))
          continue;
        if (!REFUSED_ON_PURPOSE.containsKey(relation)) {
          unaccounted.add(relation);
          continue;
        }
        // a relation refused on purpose is refused, with the 0A000 that names it
        assertThat(PostgresCatalog.resolve("select * from " + qualified(relation), database, USER)).as(relation)
            .isSameAs(PostgresCatalog.DECLINED);
      }
      assertThat(unaccounted).as("relations %s reads that are neither modelled nor refused on purpose", client).isEmpty();
    }

    // ------------------------------------------------------------------------------------------------- question 3: pg_depend

    // Regression: #10842
    /** An index that backs no constraint depends on each column of its key, automatically. */
    @Test
    void aPlainIndexDependsOnTheColumnOfItsKey() {
      final int table = oidOfClass("Account");
      final int index = oidOfClass("Account[note]");
      final int note = attnum(table, "note");

      final List<Map<String, Object>> rows = resolve(
          "select classid, objsubid, refclassid, refobjid, refobjsubid, deptype from pg_depend where objid = " + index).rows;
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst()).containsEntry("classid", PG_CLASS).containsEntry("objsubid", 0).containsEntry("refclassid", PG_CLASS)
          .containsEntry("refobjid", table).containsEntry("refobjsubid", note).containsEntry("deptype", "a");
    }

    // Regression: #10842
    /** An index that backs a unique constraint depends on that constraint internally, and the constraint on each key column. */
    @Test
    void aConstraintIndexDependsOnItsConstraintAndTheConstraintOnTheColumns() {
      final int table = oidOfClass("Account");
      final int index = oidOfClass("Account[code,region]");
      final int constraint = number(resolve("select oid from pg_constraint where conindid = " + index).rows.getFirst(), "oid");

      final List<Map<String, Object>> ofIndex = resolve(
          "select classid, refclassid, refobjid, refobjsubid, deptype from pg_depend where objid = " + index).rows;
      assertThat(ofIndex).hasSize(1);
      assertThat(ofIndex.getFirst()).containsEntry("classid", PG_CLASS).containsEntry("refclassid", PG_CONSTRAINT)
          .containsEntry("refobjid", constraint).containsEntry("refobjsubid", 0).containsEntry("deptype", "i");

      final List<Map<String, Object>> ofConstraint = resolve(
          "select classid, refclassid, refobjid, refobjsubid, deptype from pg_depend where objid = " + constraint
              + " order by refobjsubid").rows;
      assertThat(ofConstraint).extracting(row -> row.get("refobjsubid")).containsExactly(attnum(table, "code"), attnum(table, "region"));
      for (final Map<String, Object> row : ofConstraint)
        assertThat(row).containsEntry("classid", PG_CONSTRAINT).containsEntry("refclassid", PG_CLASS).containsEntry("refobjid", table)
            .containsEntry("deptype", "a");
    }

    // Regression: #10842
    /** A key that is no column of the table is an expression: the index depends on the whole table, refobjsubid 0. */
    @Test
    void anExpressionIndexDependsOnTheWholeTable() {
      final List<Map<String, Object>> rows = resolve(
          "select refobjid, refobjsubid, deptype from pg_depend where objid = " + oidOfClass("Account[secret]")).rows;
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst()).containsEntry("refobjid", oidOfClass("Account")).containsEntry("refobjsubid", 0)
          .containsEntry("deptype", "a");
    }

    // Regression: #10842
    /** The walk a tool makes from a table to what is built on it: every index, directly or through its constraint. */
    @Test
    void walkingFromATableReachesEveryIndexOfIt() {
      final int table = oidOfClass("Account");
      final Set<String> reached = new TreeSet<>();
      for (final Map<String, Object> row : resolve("""
          select c.relname from pg_depend d join pg_class c on c.oid = d.objid
          where d.classid = 1259 and d.refclassid = 1259 and d.refobjid = %d and d.deptype = 'a'
          union
          select c.relname from pg_depend d join pg_depend i on i.refobjid = d.objid join pg_class c on c.oid = i.objid
          where d.classid = 2606 and d.refobjid = %d and i.deptype = 'i'""".formatted(table, table)).rows)
        reached.add((String) row.get("relname"));
      assertThat(reached).containsExactly("Account[code,region]", "Account[note]", "Account[secret]");

      // a table with no index has no index depending on it: only its row type does (issue #10961)
      assertThat(resolve("select * from pg_depend where refobjid = " + oidOfClass("Plain") + " and classid = " + PG_CLASS).rows).isEmpty();
    }

    // Regression: #10842
    /**
     * psql's own read of pg_depend (describe.c, the column that owns a sequence), which names the catalogs with a {@code regclass}
     * cast: its FROM and WHERE verbatim, answered now that pg_depend has rows to filter. Asked of an index, it names the column the
     * index is on. The projection is psql's without its {@code quote_ident}, which this catalog does not evaluate (and which
     * PostgresCatalogTest uses as its example of a function it cannot read); psql only asks this of a sequence, which ArcadeDB has none of.
     */
    @Test
    void psqlsDependencyQueryWithRegclassCastsIsAnswered() {
      final List<Map<String, Object>> rows = resolve("""
          SELECT nspname, relname, attname, d.deptype
          FROM pg_catalog.pg_class c
          INNER JOIN pg_catalog.pg_depend d ON c.oid=d.refobjid
          INNER JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
          INNER JOIN pg_catalog.pg_attribute a ON (
           a.attrelid=c.oid AND
           a.attnum=d.refobjsubid)
          WHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass
           AND d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass
           AND d.objid='%d'
           AND d.deptype IN ('a', 'i')""".formatted(oidOfClass("Account[note]"))).rows;
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst()).containsEntry("relname", "Account").containsEntry("attname", "note").containsEntry("deptype", "a");
    }

    // Regression: #10842
    /** The catalogs pg_depend points into are named by a regclass cast in both directions, as PostgreSQL names them. */
    @Test
    void theCatalogsPgDependNamesAreRegclassNames() {
      final Map<String, Object> row = resolve("""
          select 'pg_class'::regclass as c, 'pg_catalog.pg_constraint'::pg_catalog.regclass as k, 'other.pg_class'::regclass as o,
          1259::regclass as cn, 2606::regclass as kn
          from pg_namespace limit 1""").rows.getFirst();
      assertThat(row).containsEntry("c", PG_CLASS).containsEntry("k", PG_CONSTRAINT).containsEntry("cn", "pg_class")
          .containsEntry("kn", "pg_constraint");
      // a schema other than pg_catalog is not the catalog's
      assertThat(row.get("o")).isNull();
    }

    // Regression: #10842
    /** Every OID pg_depend names is a row of the catalog it says it is in. */
    @Test
    void everyObjectPgDependNamesExists() {
      final Set<Integer> classes = oids("select oid from pg_class");
      final Set<Integer> constraints = oids("select oid from pg_constraint");
      final Set<Integer> namespaces = oids("select oid from pg_namespace");
      final Set<Integer> types = oids("select oid from pg_type");
      final List<Map<String, Object>> rows = resolve("select * from pg_depend").rows;
      // the constraint index: its constraint, and the constraint's two columns; the plain index's column; the expression index's table;
      // and for each of the two tables its schema and its row type on it (issue #10961)
      assertThat(rows).hasSize(9);
      for (final Map<String, Object> row : rows) {
        assertThat(of(number(row, "classid"), classes, constraints, namespaces, types)).as("objid of %s", row)
            .contains(number(row, "objid"));
        assertThat(of(number(row, "refclassid"), classes, constraints, namespaces, types)).as("refobjid of %s", row)
            .contains(number(row, "refobjid"));
      }
      // and the count agrees with the rows
      assertThat(number(resolve("select count(*) as n from pg_depend").rows.getFirst(), "n")).isEqualTo(9);
    }

    // Regression: #10842
    /** The columns of pg_depend are announced with PostgreSQL's types: oid and int4, and deptype as text. */
    @Test
    void pgDependAnnouncesItsColumnsAsIntegers() {
      final PostgresCatalog.Answer answer = resolve("select classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype from pg_depend");
      for (final String column : List.of("classid", "objid", "objsubid", "refclassid", "refobjid", "refobjsubid"))
        assertThat(answer.columns.get(column)).as(column).isEqualTo(PostgresType.INTEGER);
      assertThat(answer.columns.get("deptype")).isEqualTo(PostgresType.VARCHAR);
    }

    // ------------------------------------------------------------------------------------------------- helpers

    /** The relation as a query names it: a {@code pg_*} relation in {@code pg_catalog}, an {@code information_schema} one as it is. */
    static String qualified(final String relation) {
      return relation.startsWith("information_schema.") ? relation : "pg_catalog." + relation;
    }

    private static Set<Integer> of(final int catalog, final Set<Integer> classes, final Set<Integer> constraints,
        final Set<Integer> namespaces, final Set<Integer> types) {
      return switch (catalog) {
        case PG_CLASS -> classes;
        case PG_CONSTRAINT -> constraints;
        case PG_NAMESPACE -> namespaces;
        case PG_TYPE -> types;
        default -> Set.of();
      };
    }

    private Set<Integer> oids(final String query) {
      final Set<Integer> oids = new HashSet<>();
      for (final Map<String, Object> row : resolve(query).rows)
        oids.add(number(row, "oid"));
      return oids;
    }

    private int oidOfClass(final String name) {
      final List<Map<String, Object>> rows = resolve("select oid from pg_class where relname = '" + name + "'").rows;
      assertThat(rows).as(name).hasSize(1);
      return number(rows.getFirst(), "oid");
    }

    private int attnum(final int table, final String column) {
      final List<Map<String, Object>> rows = resolve(
          "select attnum from pg_attribute where attrelid = " + table + " and attname = '" + column + "'").rows;
      assertThat(rows).as(column).hasSize(1);
      return number(rows.getFirst(), "attnum");
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

  /**
   * Issue #10839: a FROM entry of a different relation that no condition joins to the others must multiply the rows, as it does in
   * PostgreSQL. The one-row-per-entity path answered such a statement with one row per entity of the most specific family, every
   * other entry reading the map of that same row, so {@code count(*)} over {@code pg_class, pg_namespace} counted the classes, and
   * the work budget never saw the product.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class CrossAndSelfJoins {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogRelationsTest-CrossAndSelfJoins";
    private static final String USER          = "root";

    private Database database;

    @BeforeEach
    void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();

      database = factory.create();
      database.transaction(() -> {
        final Schema schema = database.getSchema();
        final VertexType person = schema.createVertexType("Person");
        person.createProperty("name", Type.STRING);
        person.createProperty("age", Type.INTEGER);
        schema.createVertexType("City").createProperty("name", Type.STRING);
        schema.createDocumentType("Note").createProperty("text", Type.STRING);
      });
    }

    @AfterEach
    void dropDatabase() {
      if (database != null && database.isOpen())
        database.drop();
    }

    // ---------------------------------------------------------------- a statement that reads no entry: only its cardinality

    // Regression: #10839
    @Test
    void countOverACommaJoinOfTwoRelationsCountsTheProduct() {
      final long classes = count("select count(*) from pg_class");
      final long namespaces = count("select count(*) from pg_namespace");
      final long attributes = count("select count(*) from pg_attribute");
      final long types = count("select count(*) from pg_type");
      // the fixture must make a missing product visible: one relation of more than one row on each side
      assertThat(classes).isGreaterThan(1);
      assertThat(namespaces).isGreaterThan(1);

      assertThat(count("select count(*) from pg_class a, pg_namespace b")).isEqualTo(classes * namespaces);
      assertThat(count("select count(*) from pg_catalog.pg_namespace b, pg_catalog.pg_class a")).isEqualTo(classes * namespaces);
      assertThat(count("select count(*) from pg_class c, pg_attribute a")).isEqualTo(classes * attributes);
      assertThat(count("select count(*) from pg_namespace n, pg_type t")).isEqualTo(namespaces * types);
      assertThat(count("select count(*) from pg_class c, pg_namespace n, pg_attribute a")).isEqualTo(
          classes * namespaces * attributes);
    }

    // Regression: #10839
    @Test
    void countOverACrossJoinCountsTheProduct() {
      final long classes = count("select count(*) from pg_class");
      final long namespaces = count("select count(*) from pg_namespace");

      assertThat(count("select count(*) from pg_class c cross join pg_namespace n")).isEqualTo(classes * namespaces);
      // an inner join whose condition reads no column is a cross join too
      assertThat(count("select count(*) from pg_class c join pg_namespace n on true")).isEqualTo(classes * namespaces);
    }

    // Regression: #10839
    @Test
    void aProjectionOfLiteralsHasOneRowPerPairOfRows() {
      final long classes = count("select count(*) from pg_class");
      final long namespaces = count("select count(*) from pg_namespace");

      assertThat(resolve("select 1 as one from pg_class c, pg_namespace n").rows).hasSize((int) (classes * namespaces));
    }

    // Regression: #10839
    @Test
    void aDerivedTableOverACrossJoinHasTheProductOfRows() {
      final long classes = count("select count(*) from pg_class");
      final long namespaces = count("select count(*) from pg_namespace");

      assertThat(count("select count(*) from (select 1 as one from pg_class c, pg_namespace n) t")).isEqualTo(classes * namespaces);
    }

    // ---------------------------------------------------------------- a statement whose entries are joined: answered as it was

    // Regression: #10839
    @Test
    void aJoinOnTheNaturalKeyStillHasOneRowPerClass() {
      final long classes = count("select count(*) from pg_class");

      assertThat(count("select count(*) from pg_class c join pg_namespace n on c.relnamespace = n.oid")).isEqualTo(classes);
      assertThat(count("select count(*) from pg_class c, pg_namespace n where c.relnamespace = n.oid")).isEqualTo(classes);
      // an unqualified column is read too: relnamespace is pg_class's, and the condition joins the two entries
      assertThat(count("select count(*) from pg_class c, pg_namespace n where relnamespace = n.oid")).isEqualTo(classes);
    }

    // ---------------------------------------------------------------- the work budget sees the product of an unjoined entry

    // Regression: #10839
    @Test
    void aReadCrossJoinOfDifferentRelationsIsChargedToTheWorkBudget() {
      database.transaction(() -> {
        final VertexType wide = database.getSchema().createVertexType("Wide");
        for (int i = 0; i < 60; i++)
          wide.createProperty("p" + i, Type.STRING);
      });

      final long product = count("select count(*) from pg_attribute") * count("select count(*) from information_schema.columns")
          * count("select count(*) from pg_type") * count("select count(*) from pg_class") * count("select count(*) from pg_tables")
          * count("select count(*) from pg_namespace") * count("select count(*) from information_schema.schemata");
      // what PostgreSQL would answer is more rows than one statement may spend
      assertThat(product).isGreaterThan(5_000_000L);

      final String unjoined = "select a.attname, col.column_name, t.typname, c.relname, tb.tablename, n.nspname, s.schema_name"
          + " from pg_attribute a, information_schema.columns col, pg_type t, pg_class c, pg_tables tb, pg_namespace n,"
          + " information_schema.schemata s";
      assertThatThrownBy(() -> PostgresCatalog.resolve(unjoined, database, USER)).isInstanceOf(
          PostgresCatalog.WorkLimitExceeded.class);

      // and a filter on one entry only does not join the others to it
      assertThatThrownBy(() -> PostgresCatalog.resolve(unjoined + " where c.relname = 'Wide'", database, USER)).isInstanceOf(
          PostgresCatalog.WorkLimitExceeded.class);
    }

    // Regression: #10839
    @Test
    void aJoinedStatementOverTheSameRelationsIsNotCharged() {
      database.transaction(() -> {
        final VertexType wide = database.getSchema().createVertexType("Wide");
        for (int i = 0; i < 60; i++)
          wide.createProperty("p" + i, Type.STRING);
      });

      // the JDBC driver's getColumns() shape: every entry joined on its key, one row per column
      final List<Map<String, Object>> rows = resolve("select n.nspname, c.relname, a.attname from pg_namespace n"
          + " join pg_class c on c.relnamespace = n.oid join pg_attribute a on a.attrelid = c.oid"
          + " where c.relname = 'Wide' and a.attnum > 0").rows;
      assertThat(rows).hasSize(60);
    }

    /*
     * Issue #10593: a statement that reads one catalog relation through two aliases must bind each alias to its own rows.
     * <ul>
     * <li>A second alias on the right of a LEFT JOIN was answered on the one-row-per-entity path, where both aliases share one map, so a
     * condition that never holds within one row ({@code arr.oid = t.typarray}) read as "no match" and the client got NULL for a value
     * the comma and INNER JOIN spellings of the same question return.</li>
     * <li>An alias nothing reads was dropped from the FROM list, so {@code count(*)} over a self cross join counted one relation, and
     * the statement work budget never saw the product it is meant to stop.</li>
     * </ul>
     *
     * @author Roberto Franchini (r.franchini@arcadedata.com)
     */
    // ---------------------------------------------------------------- the LEFT JOIN spelling of a self join

    // Regression: #10593
    @Test
    void leftJoinToTheArrayTypeAnswersTheArrayType() {
      final List<Map<String, Object>> rows = resolve("select t.typname, arr.typname as arrname from pg_catalog.pg_type t"
          + " left join pg_catalog.pg_type arr on arr.oid = t.typarray where t.typname = 'int4'").rows;
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).get("typname")).isEqualTo("int4");
      assertThat(rows.get(0).get("arrname")).isEqualTo("_int4");
    }

    // Regression: #10593
    @Test
    void leftJoinToTheElementTypeAnswersTheElementType() {
      final List<Map<String, Object>> rows = resolve("select t.typname, e.typname as elemname from pg_catalog.pg_type t"
          + " left join pg_catalog.pg_type e on e.oid = t.typelem where t.typname = '_int4'").rows;
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).get("typname")).isEqualTo("_int4");
      assertThat(rows.get(0).get("elemname")).isEqualTo("int4");
    }

    // Regression: #10593
    /** The differential the report asks for: the comma, INNER JOIN and LEFT JOIN spellings of one question agree where it matches. */
    @Test
    void theThreeSpellingsOfASelfJoinAgree() {
      final String comma = "select t.typname, arr.typname as arrname from pg_catalog.pg_type t, pg_catalog.pg_type arr"
          + " where t.typarray = arr.oid order by t.typname";
      final String inner = "select t.typname, arr.typname as arrname from pg_catalog.pg_type t"
          + " join pg_catalog.pg_type arr on arr.oid = t.typarray order by t.typname";
      final String left = "select t.typname, arr.typname as arrname from pg_catalog.pg_type t"
          + " left join pg_catalog.pg_type arr on arr.oid = t.typarray where arr.oid is not null order by t.typname";

      final List<Map<String, Object>> commaRows = resolve(comma).rows;
      assertThat(commaRows).isNotEmpty();
      assertThat(resolve(inner).rows).isEqualTo(commaRows);
      assertThat(resolve(left).rows).isEqualTo(commaRows);

      // the outer join adds exactly the types with no array type, each with a NULL partner
      final List<Map<String, Object>> all = resolve("select t.typname, arr.typname as arrname from pg_catalog.pg_type t"
          + " left join pg_catalog.pg_type arr on arr.oid = t.typarray").rows;
      final long types = count("select count(*) from pg_catalog.pg_type");
      assertThat(all).hasSize((int) types);
      assertThat(all).filteredOn(row -> row.get("arrname") == null).hasSize((int) (types - commaRows.size()));
    }

    // Regression: #10593
    /** The form the old guard was written for: psql's TOAST lookup, which matches nothing and must still read NULL. */
    @Test
    void leftJoinToASecondAliasThatMatchesNothingReadsNull() {
      final List<Map<String, Object>> rows = resolve("select c.relname, tc.relname as toast from pg_catalog.pg_class c"
          + " left join pg_catalog.pg_class tc on (c.reltoastrelid = tc.oid) where c.relname = 'Person'").rows;
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).get("relname")).isEqualTo("Person");
      assertThat(rows.get(0).get("toast")).isNull();
    }

    // Regression: #10593
    /** A second alias that matches its own row - the guard's live branch - is answered with that row rather than declined. */
    @Test
    void leftJoinToASecondAliasThatMatchesTheSameRow() {
      final List<Map<String, Object>> rows = resolve("select c.relname, c2.relname as same from pg_catalog.pg_class c"
          + " left join pg_catalog.pg_class c2 on c2.oid = c.oid where c.relname = 'City'").rows;
      assertThat(rows).hasSize(1);
      assertThat(rows.get(0).get("same")).isEqualTo("City");
    }

    // ---------------------------------------------------------------- an unread FROM entry still multiplies

    // Regression: #10593
    @Test
    void anUnreadSecondAliasMultipliesTheRows() {
      final long classes = count("select count(*) from pg_catalog.pg_class");
      assertThat(classes).isGreaterThan(1);
      assertThat(count("select count(*) from pg_class a, pg_class b")).isEqualTo(classes * classes);
      // the spellings of one relation are one relation: with or without its schema, in any case
      assertThat(count("select count(*) from pg_catalog.pg_class a, pg_class b")).isEqualTo(classes * classes);
      assertThat(count("select count(*) from PG_Class a, pg_class b")).isEqualTo(classes * classes);
      assertThat(count("select count(*) from pg_class a, pg_class b where a.oid > 0 and b.oid > 0")).isEqualTo(classes * classes);
      assertThat(resolve("select a.relname as x, b.relname as y from pg_class a, pg_class b").rows).hasSize((int) (classes * classes));
      assertThat(count("select count(*) from (select a.relname as x, b.relname as y from pg_class a, pg_class b) t"))
          .isEqualTo(classes * classes);
    }

    // Regression: #10593
    @Test
    void anUnreadCrossJoinIsChargedToTheWorkBudget() {
      // enough columns that six of pg_attribute pair more rows than one statement may spend: at least 20^6, 64 million
      database.transaction(() -> {
        final VertexType wide = database.getSchema().createVertexType("Wide");
        for (int i = 0; i < 16; i++)
          wide.createProperty("p" + i, Type.STRING);
      });
      final long attributes = count("select count(*) from pg_catalog.pg_attribute");
      assertThat(attributes).isGreaterThanOrEqualTo(20);

      final String unread = "select count(*) from pg_attribute a, pg_attribute b, pg_attribute c, pg_attribute d,"
          + " pg_attribute e, pg_attribute f";
      final String read = unread + " where a.attnum > 0 and b.attnum > 0 and c.attnum > 0 and d.attnum > 0 and e.attnum > 0"
          + " and f.attnum > 0";
      assertThatThrownBy(() -> PostgresCatalog.resolve(read, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
      assertThatThrownBy(() -> PostgresCatalog.resolve(unread, database, USER)).isInstanceOf(PostgresCatalog.WorkLimitExceeded.class);
    }

    // ---------------------------------------------------------------- helpers

    private long count(final String query) {
      final List<Map<String, Object>> rows = resolve(query).rows;
      assertThat(rows).as("one row: %s", query).hasSize(1);
      return ((Number) rows.get(0).values().iterator().next()).longValue();
    }

    private PostgresCatalog.Answer resolve(final String query) {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
      assertThat(answer).as("answered: %s", query).isNotNull();
      assertThat(answer.rows).as("answered rather than declined: %s", query).isNotNull();
      return answer;
    }
  }

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
  @Nested
  class OwnerJoins {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogRelationsTest-OwnerJoins";
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

    // Regression: #7868
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

    // Regression: #7868
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

    // Regression: #7868
    @Test
    void aColumnRowATypeRowAndASchemaRowAllCarryTheOwnerAsWell() {
      assertThat(resolve("SELECT a.attname, r.rolname FROM pg_attribute a, pg_roles r").rows)
          .allSatisfy(row -> assertThat(row.get("rolname")).isEqualTo(USER));

      final PostgresCatalog.Answer types = resolve(
          "SELECT t.typname, t.typowner, r.oid, r.rolname FROM pg_type t JOIN pg_roles r ON r.oid = t.typowner");
      // the built-in types and the row type of the one relation (issue #10873)
      assertThat(types.rows).hasSize(PostgresTypeCatalog.types().size() + 1);
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

    // Regression: #7868
    /**
     * The ranking half. A single-row relation joined to pg_namespace reads its one row on every schema, so the answer is never one
     * nameless role - the #7224 outcome. pg_namespace has two rows since issue #10745 - pg_catalog, which every type names in
     * typnamespace, and the database's schema - so the join is the owner of each schema, as in PostgreSQL.
     */
    @Test
    void aRoleQueryThatNamesPgNamespaceIsStillAboutRoles() {
      final PostgresCatalog.Answer answer = resolve("SELECT r.rolname FROM pg_roles r JOIN pg_namespace n ON n.nspowner = r.oid");

      assertThat(names(answer.rows, "rolname")).as("the owner of each schema, never a nameless role").containsExactly(USER, USER);
    }

    // Regression: #7868
    @Test
    void theOtherFormerlyUnrankedFamiliesNoLongerLoseToPgNamespace() {
      assertThat(names(resolve("SELECT d.datname FROM pg_database d, pg_namespace n").rows, "datname"))
          .containsExactly(database.getName(), database.getName());

      assertThat(names(resolve(
          "SELECT p.privilege_type FROM information_schema.usage_privileges p, pg_namespace n").rows, "privilege_type"))
          .containsExactly("USAGE", "USAGE");

      assertThat(names(resolve(
          "SELECT c.character_set_name FROM information_schema.character_sets c, pg_namespace n").rows, "character_set_name"))
          .containsExactly("UTF8", "UTF8");

      assertThat(names(resolve(
          "SELECT c.collation_name FROM information_schema.collations c, pg_namespace n").rows, "collation_name"))
          .containsExactly("default", "default");
    }

    // Regression: #7868
    /** The FROM order must not decide it, for these families any more than it does for TYPES. */
    @Test
    void theFromOrderDoesNotChangeTheAnswer() {
      assertThat(resolve("SELECT r.rolname, n.nspname FROM pg_namespace n, pg_roles r").rows)
          .containsExactlyElementsOf(resolve("SELECT r.rolname, n.nspname FROM pg_roles r, pg_namespace n").rows);
    }

    // Regression: #7868
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
        // one row per schema, the one relation of the six with two rows (issue #10745), each carrying the five singletons
        assertThat(names(answer.rows, "nspname")).as("one row per schema, whichever relation is named first")
            .containsExactlyInAnyOrder("pg_catalog", database.getName());
        for (final Map<String, Object> row : answer.rows)
          assertThat(row).as("no column may fall through to Row.complete()'s null fill (first=%s)", relations[first])
              .doesNotContainValue(null);
      }
    }

    // Regression: #7868
    /** What must not move: the families that CONSTRAIN a row set still outrank the ones that only qualify it. */
    @Test
    void tablesColumnsAndTypesStillOutrankTheSingleRowFamilies() {
      assertThat(names(resolve("SELECT c.relname, r.rolname FROM pg_class c, pg_roles r").rows, "relname"))
          .containsExactly("Article");
      assertThat(names(resolve("SELECT a.attname, r.rolname FROM pg_attribute a, pg_roles r").rows, "attname"))
          .contains("id");
      assertThat(resolve("SELECT t.typname, r.rolname FROM pg_roles r, pg_type t").rows)
          .hasSize(PostgresTypeCatalog.types().size() + 1);
    }

    // Regression: #7868
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

      // the built-in types, in pg_catalog, and the row type of the one relation (issue #10873), which is in the relation's schema
      assertThat(answer.rows).hasSize(PostgresTypeCatalog.types().size() + 1);
      assertThat(answer.rows).filteredOn(row -> "Article".equals(row.get("typname"))).singleElement().satisfies(row -> {
        assertThat(row.get("nspname")).isEqualTo(database.getName());
        assertThat(row.get("schema_name")).isEqualTo(database.getName());
        assertThat(row.get("object_schema")).isEqualTo(database.getName());
      });
      assertThat(answer.rows).filteredOn(row -> !"Article".equals(row.get("typname"))).allSatisfy(row -> {
        assertThat(row.get("nspname")).isEqualTo("pg_catalog");
        assertThat(row.get("schema_name")).as("schemata is the information_schema spelling of pg_namespace")
            .isEqualTo("pg_catalog");
        assertThat(row.get("object_schema")).isEqualTo("pg_catalog");
        assertThat(row.get("object_name")).isEqualTo("pg_catalog");
        // A catalog is a DATABASE, not a schema, so these stay put whichever schema the row describes.
        assertThat(row.get("catalog_name")).isEqualTo(database.getName());
        assertThat(row.get("object_catalog")).isEqualTo(database.getName());
      });

      // And a row that is NOT about a type names its own schema everywhere: pg_catalog's row and the database's (issue #10745)
      final PostgresCatalog.Answer schemas = resolve(
          "SELECT n.nspname, s.schema_name, p.object_schema FROM pg_namespace n, information_schema.schemata s, "
              + "information_schema.usage_privileges p");
      assertThat(names(schemas.rows, "nspname")).containsExactly("pg_catalog", database.getName());
      assertThat(schemas.rows).allSatisfy(row -> {
        assertThat(row.get("schema_name")).isEqualTo(row.get("nspname"));
        assertThat(row.get("object_schema")).isEqualTo(row.get("nspname"));
      });
    }

    // Regression: #7868
    /** And pg_namespace alone is still a question about schemas: pg_catalog and the database's own (issue #10745). */
    @Test
    void pgNamespaceOnItsOwnIsStillASchemaQuery() {
      final PostgresCatalog.Answer answer = resolve("SELECT nspname FROM pg_catalog.pg_namespace");

      assertThat(names(answer.rows, "nspname")).containsExactly("pg_catalog", database.getName());
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
}
