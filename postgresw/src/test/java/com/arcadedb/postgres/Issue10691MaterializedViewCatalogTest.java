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
 * Issue #10691: the catalog built its relations from {@code Schema.getTypes()} and classified every one of them as an ordinary
 * table, so the backing type of a materialized view - and of a continuous aggregate, which is created the same way - was listed
 * as a {@code BASE TABLE} with {@code relkind = 'r'}, while {@code pg_matviews} was refused and {@code \dm} answered nothing.
 * <p>
 * A materialized view is described here where PostgreSQL describes one: {@code pg_class} with {@code relkind = 'm'} and a row of
 * {@code pg_matviews}. PostgreSQL lists none in {@code pg_tables}, {@code pg_views}, {@code information_schema.tables} or
 * {@code information_schema.views}: the first two hold kinds 'r' and 'v', the last two the kinds 'r', 'v', 'f' and 'p'.
 */
class Issue10691MaterializedViewCatalogTest {
  private static final String DATABASE_PATH   = "./target/databases/Issue10691MaterializedViewCatalogTest";
  private static final String USER            = "root";
  private static final String VIEW_QUERY      = "SELECT name FROM Person";
  private static final String AGGREGATE_QUERY = "SELECT ts.timeBucket('1h', ts) AS h, sum(v) AS s FROM Reading GROUP BY h";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      schema.createVertexType("Person").createProperty("name", Type.STRING);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Person", "name");
    });
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a'"));
    database.command("sql", "CREATE MATERIALIZED VIEW PersonView AS " + VIEW_QUERY);

    database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (sensor STRING) FIELDS (v DOUBLE)");
    database.transaction(() -> database.command("sql", "INSERT INTO Reading SET ts = ?, sensor = 'a', v = 1.0", 1_700_000_000_000L));
    database.command("sql", "CREATE CONTINUOUS AGGREGATE AggView AS " + AGGREGATE_QUERY);
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /** The report's own query: a materialized view and a continuous aggregate are relations of kind 'm', not tables. */
  @Test
  void pgClassClassifiesEachRelationByWhatItIs() {
    final PostgresCatalog.Answer answer = resolve("select relname, relkind from pg_class where relkind <> 'i' order by 1");

    assertThat(column(answer, "relname")).containsExactly("AggView", "Person", "PersonView", "Reading");
    assertThat(column(answer, "relkind")).containsExactly("m", "r", "m", "r");
  }

  @Test
  void theTableListsHoldOnlyTheTables() {
    assertThat(column(resolve("select tablename from pg_tables order by 1"), "tablename")).containsExactly("Person", "Reading");
    final PostgresCatalog.Answer tables = resolve("select table_name, table_type from information_schema.tables order by 1");
    assertThat(column(tables, "table_name")).containsExactly("Person", "Reading");
    assertThat(column(tables, "table_type")).containsOnly("BASE TABLE");
  }

  /** psql 17's {@code \dm}: the materialized views by their relkind, which used to answer nothing. */
  @Test
  void psqlListsTheMaterializedViews() {
    final PostgresCatalog.Answer answer = resolve("""
        SELECT n.nspname as "Schema",
          c.relname as "Name",
          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
          pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE c.relkind IN ('m','')
              AND n.nspname <> 'pg_catalog'
              AND n.nspname !~ '^pg_toast'
              AND n.nspname <> 'information_schema'
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 1,2;
        """);

    assertThat(column(answer, "Name")).containsExactly("AggView", "PersonView");
    assertThat(column(answer, "Type")).containsOnly("materialized view");
    assertThat(column(answer, "Owner")).containsOnly(USER);
  }

  /** psql 17's {@code \dt}: a materialized view is not a table, and is no longer listed as one. */
  @Test
  void psqlListsOnlyTheTablesAsTables() {
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
        ORDER BY 1,2;
        """);

    assertThat(column(answer, "Name")).containsExactly("Person", "Reading");
    assertThat(column(answer, "Type")).containsOnly("table");
  }

  @Test
  void pgMatviewsListsEachViewWithItsDefinition() {
    final PostgresCatalog.Answer answer = resolve(
        "select schemaname, matviewname, matviewowner, hasindexes, ispopulated, definition from pg_catalog.pg_matviews order by 2");

    assertThat(column(answer, "matviewname")).containsExactly("AggView", "PersonView");
    assertThat(column(answer, "schemaname")).containsOnly(database.getName());
    assertThat(column(answer, "matviewowner")).containsOnly(USER);
    // follows pg_index (issue #10473): the backing type of a continuous aggregate is indexed on its bucket column (issue #10804),
    // the one of a materialized view is not (issue #10828)
    assertThat(column(answer, "hasindexes")).containsExactly(true, false);
    assertThat(column(answer, "ispopulated")).containsOnly(true);
    assertThat(column(answer, "definition")).containsExactly(AGGREGATE_QUERY, VIEW_QUERY);
  }

  /** An ungrouped count over pg_matviews answers its one row, as it does over every relation this catalog models. */
  @Test
  void pgMatviewsIsModelledRatherThanRefused() {
    assertThat(column(resolve("select count(*) from pg_matviews"), "count")).containsExactly(2L);
    assertThat(column(resolve("select count(*) from pg_matviews where 1 = 0"), "count")).containsExactly(0L);
  }

  /** PostgreSQL keeps a materialized view out of both view listings: they hold relkind 'v', which ArcadeDB has none of. */
  @Test
  void theViewListingsHoldNoMaterializedView() {
    assertThat(resolve("select viewname from pg_views").rows).isEmpty();
    assertThat(resolve("select table_name from information_schema.views").rows).isEmpty();
  }

  /** {@code \d+ "PersonView"} prints the view's query through pg_get_viewdef; a table has none. */
  @Test
  void pgGetViewdefPrintsTheQueryOfAMaterializedView() {
    final PostgresCatalog.Answer view = resolve("select pg_catalog.pg_get_viewdef(c.oid, true) as def from pg_class c where c.relname = 'PersonView'");
    assertThat(column(view, "def")).containsExactly(VIEW_QUERY);

    final PostgresCatalog.Answer table = resolve("select pg_catalog.pg_get_viewdef(c.oid) as def from pg_class c where c.relname = 'Person'");
    assertThat(column(table, "def")).containsExactly((Object) null);

    // the name of the relation, rather than its OID: the view's query, and NULL for a table or a name that is no relation
    assertThat(column(resolve("select pg_catalog.pg_get_viewdef('AggView') as def from pg_class where relname = 'Person'"), "def"))
        .containsExactly(AGGREGATE_QUERY);
    assertThat(column(resolve("select pg_catalog.pg_get_viewdef('Person') as def from pg_class where relname = 'Person'"), "def"))
        .containsExactly((Object) null);
    assertThat(column(resolve("select pg_catalog.pg_get_viewdef('Nothing') as def from pg_class where relname = 'Person'"), "def"))
        .containsExactly((Object) null);
  }

  /** A join between the class and the view row of one relation reads both off the same row, whichever is named first. */
  @Test
  void aJoinOfPgClassAndPgMatviewsAgrees() {
    final PostgresCatalog.Answer classFirst = resolve(
        "select c.relname, m.definition from pg_class c join pg_matviews m on m.matviewname = c.relname order by 1");
    final PostgresCatalog.Answer viewFirst = resolve(
        "select c.relname, m.definition from pg_matviews m join pg_class c on m.matviewname = c.relname order by 1");

    assertThat(column(classFirst, "relname")).containsExactly("AggView", "PersonView");
    assertThat(column(classFirst, "definition")).containsExactly(AGGREGATE_QUERY, VIEW_QUERY);
    assertThat(column(viewFirst, "relname")).isEqualTo(column(classFirst, "relname"));
    assertThat(column(viewFirst, "definition")).isEqualTo(column(classFirst, "definition"));
  }

  /**
   * The guarantee the issue asks for: every relation pg_class describes is listed by exactly one of the relations that name its
   * kind, and every type of the schema is one of them. The schema holds one of each kind this catalog knows - an ordinary type, a
   * materialized view, a continuous aggregate and an index - so a kind the engine gains next, emitted as a table again, fails here.
   */
  @Test
  void everyRelationIsListedByExactlyOneRelationOfItsKind() {
    final long classes = count("select count(*) from pg_class");
    final long tables = count("select count(*) from pg_tables");
    final long views = count("select count(*) from pg_views");
    final long matviews = count("select count(*) from pg_matviews");
    final long indexes = count("select count(*) from pg_class where relkind = 'i'");

    assertThat(tables).isEqualTo(2);
    assertThat(matviews).isEqualTo(2);
    assertThat(indexes).isPositive();
    assertThat(classes).isEqualTo(tables + views + matviews + indexes);

    assertThat(count("select count(*) from pg_class c join pg_tables t on t.tablename = c.relname where c.relkind <> 'r'")).isZero();
    assertThat(count("select count(*) from pg_class c join pg_matviews m on m.matviewname = c.relname where c.relkind <> 'm'")).isZero();

    final List<String> types = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes())
      types.add(type.getName());
    assertThat(column(resolve("select relname from pg_class where relkind <> 'i'"), "relname")).containsExactlyInAnyOrderElementsOf(
        types.stream().map(name -> (Object) name).toList());
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
