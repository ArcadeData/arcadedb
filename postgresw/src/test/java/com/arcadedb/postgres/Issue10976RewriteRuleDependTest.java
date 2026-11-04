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
 * Issue #10976: a materialized view is answered by its {@code _RETURN} rewrite rule in PostgreSQL, and {@code pg_depend} lists that
 * rule as part of the view ({@code 'i'}) and as depending on every relation the view reads ({@code 'n'}). This catalog had no
 * {@code pg_rewrite} rows, so a tool walking from a table to what is built on it ({@code pg_depend} joined to {@code pg_rewrite} on
 * the rule, then to {@code pg_class} on {@code ev_class}) found no view. It now lists the rule of each materialized view and
 * continuous aggregate, {@code pg_class.relhasrules} says so, and {@code pg_get_ruledef} prints it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10976RewriteRuleDependTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10976RewriteRuleDependTest";
  private static final String USER          = "root";
  private static final int    PG_CLASS      = 1259;
  private static final int    PG_REWRITE    = 2618;
  // READS Person IN ITS FROM AND Pet IN A SUBQUERY OF ITS WHERE: BOTH ARE RELATIONS THE RULE DEPENDS ON
  private static final String VIEW_QUERY    = "SELECT name FROM Person WHERE name IN (SELECT owner FROM Pet)";

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
      schema.createDocumentType("Pet").createProperty("owner", Type.STRING);
      schema.createDocumentType("Unrelated").createProperty("x", Type.STRING);
    });
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET name = 'a'");
      database.command("sql", "INSERT INTO Pet SET owner = 'a'");
    });
    database.command("sql", "CREATE MATERIALIZED VIEW Owners AS " + VIEW_QUERY);

    database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (sensor STRING) FIELDS (v DOUBLE)");
    database.command("sql", "CREATE CONTINUOUS AGGREGATE Hourly AS "
        + "SELECT ts.timeBucket('1h', ts) AS h, sum(v) AS s FROM Reading GROUP BY h");
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void eachMaterializedRelationHasItsReturnRule() {
    final PostgresCatalog.Answer answer = resolve("""
        select c.relname, r.rulename, r.ev_type, r.ev_enabled, r.is_instead
        from pg_rewrite r join pg_class c on c.oid = r.ev_class order by 1""");
    assertThat(column(answer, "relname")).containsExactly("Hourly", "Owners");
    assertThat(column(answer, "rulename")).containsOnly("_RETURN");
    assertThat(column(answer, "ev_type")).containsOnly("1");
    assertThat(column(answer, "ev_enabled")).containsOnly("O");
    assertThat(column(answer, "is_instead")).containsOnly(Boolean.TRUE);

    // a table has no rule, and pg_class agrees with pg_rewrite on which relations have one
    assertThat(column(resolve("select relname from pg_class where relhasrules order by 1"), "relname")).containsExactly("Hourly",
        "Owners");
    assertThat(column(resolve("select relname from pg_class where relkind = 'r' and relhasrules"), "relname")).isEmpty();
  }

  @Test
  void theRuleIsPartOfItsViewAndDependsOnWhatTheViewReads() {
    final int rule = ruleOf("Owners");
    final PostgresCatalog.Answer answer = resolve(
        "select classid, objsubid, refclassid, refobjid, refobjsubid, deptype from pg_depend where objid = " + rule
            + " order by deptype, refobjid");
    assertThat(column(answer, "classid")).containsOnly(PG_REWRITE);
    assertThat(column(answer, "refclassid")).containsOnly(PG_CLASS);
    assertThat(column(answer, "objsubid")).containsOnly(0);
    assertThat(column(answer, "refobjsubid")).containsOnly(0);

    final List<Object> internal = new ArrayList<>();
    final List<Object> normal = new ArrayList<>();
    for (final Map<String, Object> row : answer.rows)
      ("i".equals(row.get("deptype")) ? internal : normal).add(row.get("refobjid"));
    assertThat(internal).containsExactly(oidOf("Owners"));
    assertThat(normal).containsExactlyInAnyOrder(oidOf("Person"), oidOf("Pet"));

    // the continuous aggregate's rule depends on the time series it aggregates
    final PostgresCatalog.Answer aggregate = resolve(
        "select refobjid, deptype from pg_depend where objid = " + ruleOf("Hourly") + " and deptype = 'n'");
    assertThat(column(aggregate, "refobjid")).containsExactly(oidOf("Reading"));
  }

  /** The walk a dependency tool does: from a table, through the rules that depend on it, to the views those rules belong to. */
  @Test
  void aTableLeadsToTheViewsBuiltOnIt() {
    final String walk = """
        select distinct v.relname from pg_depend d
        join pg_rewrite r on r.oid = d.objid
        join pg_class v on v.oid = r.ev_class
        join pg_class t on t.oid = d.refobjid
        where d.classid = 'pg_rewrite'::regclass and d.refclassid = 'pg_class'::regclass and d.deptype = 'n'
          and t.relname = '%s'""";
    assertThat(column(resolve(walk.formatted("Pet")), "relname")).containsExactly("Owners");
    assertThat(column(resolve(walk.formatted("Person")), "relname")).containsExactly("Owners");
    assertThat(column(resolve(walk.formatted("Reading")), "relname")).containsExactly("Hourly");
    assertThat(resolve(walk.formatted("Unrelated")).rows).isEmpty();
  }

  @Test
  void pgGetRuledefPrintsTheRule() {
    final int rule = ruleOf("Owners");
    assertThat(column(resolve("select pg_catalog.pg_get_ruledef(" + rule + ") as def from pg_class where relname = 'Person'"),
        "def")).containsExactly("CREATE RULE \"_RETURN\" AS ON SELECT TO \"Owners\" DO INSTEAD  " + VIEW_QUERY + ";");
    assertThat(column(resolve("select pg_catalog.pg_get_ruledef(" + rule + ", true) as def from pg_class where relname = 'Person'"),
        "def")).containsExactly("CREATE RULE \"_RETURN\" AS\n    ON SELECT TO \"Owners\" DO INSTEAD  " + VIEW_QUERY + ";");
    // a lower-case name needs no quotes, as quote_ident prints it
    database.command("sql", "CREATE MATERIALIZED VIEW owners2 AS SELECT name FROM Person");
    assertThat(column(resolve("select pg_catalog.pg_get_ruledef(" + ruleOf("owners2") + ") as def from pg_class where relname = 'Person'"),
        "def")).containsExactly("CREATE RULE \"_RETURN\" AS ON SELECT TO owners2 DO INSTEAD  SELECT name FROM Person;");
    // an OID that names no rule: a table's own OID
    assertThat(column(resolve("select pg_catalog.pg_get_ruledef(oid) as def from pg_class where relname = 'Person'"), "def"))
        .containsExactly((Object) null);
  }

  @Test
  void aDroppedViewTakesItsRuleAndItsDependenciesWithIt() {
    final int rule = ruleOf("Owners");
    database.command("sql", "DROP MATERIALIZED VIEW Owners");
    assertThat(resolve("select oid from pg_rewrite where oid = " + rule).rows).isEmpty();
    assertThat(resolve("select deptype from pg_depend where objid = " + rule).rows).isEmpty();
  }

  // ---------------------------------------------------------------- helpers

  private int ruleOf(final String view) {
    final List<Object> rules = column(
        resolve("select r.oid from pg_rewrite r join pg_class c on c.oid = r.ev_class where c.relname = '" + view + "'"), "oid");
    assertThat(rules).as("the rule of %s", view).hasSize(1);
    return ((Number) rules.get(0)).intValue();
  }

  private Object oidOf(final String relation) {
    final List<Object> oids = column(resolve("select oid from pg_class where relname = '" + relation + "'"), "oid");
    assertThat(oids).as("the relation %s", relation).hasSize(1);
    return oids.get(0);
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
