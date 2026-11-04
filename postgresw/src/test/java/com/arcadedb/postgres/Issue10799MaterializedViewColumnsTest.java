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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10799: the backing type of a materialized view or of a continuous aggregate declares no property, so the catalog, which
 * built a relation's columns from the declared properties of its type, described the view with {@code relnatts = 0} and no
 * {@code pg_attribute} row: {@code \d "PersonView"} printed a relation with no column.
 * <p>
 * A view's columns are the ones its query projects, in the order it projects them, typed as a statement Describe of that query
 * types them; a property the backing type does declare - the bucket of a continuous aggregate - is typed from its declaration.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10799MaterializedViewColumnsTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10799MaterializedViewColumnsTest";
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
      schema.createVertexType("Person").createProperty("name", Type.STRING);
      schema.getType("Person").createProperty("age", Type.INTEGER);
    });
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a', age = 3"));
    // the report's view
    database.command("sql", "CREATE MATERIALIZED VIEW PersonView AS SELECT name FROM Person");
    database.command("sql", "CREATE MATERIALIZED VIEW Projected AS SELECT age, name AS label FROM Person");
    database.command("sql", "CREATE MATERIALIZED VIEW Counted AS SELECT name, count(*) AS total FROM Person GROUP BY name");
    database.command("sql", "CREATE MATERIALIZED VIEW Everything AS SELECT FROM Person");

    database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (sensor STRING) FIELDS (v DOUBLE)");
    database.transaction(() -> database.command("sql", "INSERT INTO Reading SET ts = ?, sensor = 'a', v = 1.0", 1_700_000_000_000L));
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE Hourly AS SELECT ts.timeBucket('1h', ts) AS h, max(v) AS peak FROM Reading GROUP BY h");
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /** The report's repro: the view has the column its query projects, and relnatts counts it. */
  @Test
  void theViewHasTheColumnItsQueryProjects() {
    assertThat(column(resolve("select relnatts from pg_class where relname = 'PersonView'"), "relnatts")).containsExactly(1);
    assertThat(column(resolve("""
        select attname from pg_attribute a join pg_class c on c.oid = a.attrelid where c.relname = 'PersonView'"""), "attname"))
        .containsExactly("name");
  }

  /** In the order the query projects them, under their alias, typed from the property each reads. */
  @Test
  void theColumnsFollowTheProjection() {
    final PostgresCatalog.Answer answer = attributes("Projected");
    assertThat(column(answer, "attname")).containsExactly("age", "label");
    assertThat(column(answer, "attnum")).containsExactly(1, 2);
    assertThat(column(answer, "type")).containsExactly("integer", "character varying");
  }

  @Test
  void anAggregateIsTypedAsADescribeTypesIt() {
    final PostgresCatalog.Answer answer = attributes("Counted");
    assertThat(column(answer, "attname")).containsExactly("name", "total");
    assertThat(column(answer, "type")).containsExactly("character varying", "bigint");
  }

  /** A query with no projection stores the whole record of the source, so the view has the columns of the source. */
  @Test
  void aViewOfWholeRecordsHasTheColumnsOfItsSource() {
    final PostgresCatalog.Answer answer = attributes("Everything");
    assertThat(column(answer, "attname")).containsExactly("age", "name");
    assertThat(column(answer, "type")).containsExactly("integer", "character varying");
  }

  /** The bucket of a continuous aggregate is declared on its backing type, and its declaration is what types it. */
  @Test
  void theBucketOfAContinuousAggregateIsATimestamp() {
    final PostgresCatalog.Answer answer = attributes("Hourly");
    assertThat(column(answer, "attname")).containsExactly("h", "peak");
    assertThat(column(answer, "type")).containsExactly("timestamp without time zone", "double precision");
  }

  /** relnatts of every relation counts the pg_attribute rows of it, views included. */
  @Test
  void relnattsOfEveryViewCountsItsAttributeRows() {
    for (final Map<String, Object> view : resolve("select oid, relname, relnatts from pg_class where relkind = 'm'").rows) {
      final PostgresCatalog.Answer count = resolve("select count(*) from pg_attribute where attrelid = " + view.get("oid"));
      assertThat(((Number) count.rows.get(0).get("count")).intValue()).as("the columns of %s", view.get("relname"))
          .isEqualTo(view.get("relnatts")).isPositive();
    }
  }

  /** PostgreSQL's information_schema.columns lists the columns of tables and views, never those of a materialized view. */
  @Test
  void informationSchemaColumnsStillLeavesTheViewsOut() {
    assertThat(column(resolve("select distinct table_name from information_schema.columns order by 1"), "table_name"))
        .containsExactly("Person", "Reading");
  }

  // ---------------------------------------------------------------- helpers

  /** psql's {@code \d} column query, the one that printed no column for a view. */
  private PostgresCatalog.Answer attributes(final String relation) {
    return resolve("""
        SELECT a.attname, a.attnum, pg_catalog.format_type(a.atttypid, a.atttypmod) as type
        FROM pg_catalog.pg_attribute a
        WHERE a.attrelid = (select oid from pg_class where relname = '%s') AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum""".formatted(relation));
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
