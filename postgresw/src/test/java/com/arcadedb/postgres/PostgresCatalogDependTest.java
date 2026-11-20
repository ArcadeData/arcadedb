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
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The dependencies and references of the emulated PostgreSQL catalog: the {@code pg_depend} rows of tables and rewrite rules, and the
 * reference columns ({@code typarray}, {@code typelem}, {@code typnamespace}, ...) that must name a row the catalog has.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresCatalogDependTest {

  /**
   * Issue #10961: {@code pg_depend} lists the dependencies a stock PostgreSQL records for a user table beside the ones of its indexes
   * (issue #10842): the table on its schema ({@code deptype 'n'}) and the row type of the table on the table ({@code 'i'}).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TableDepend {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogDependTest-TableDepend";
    private static final String USER          = "root";

    private static final int PG_CLASS     = 1259;
    private static final int PG_TYPE      = 1247;
    private static final int PG_NAMESPACE = 2615;

    private Database database;

    @BeforeEach
    void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();

      database = factory.create();
      database.transaction(() -> {
        final Schema schema = database.getSchema();
        final DocumentType account = schema.createDocumentType("Account");
        account.createProperty("note", Type.STRING);
        schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Account", "note");
        schema.createDocumentType("Plain").createProperty("name", Type.STRING);
      });
    }

    @AfterEach
    void dropDatabase() {
      if (database != null && database.isOpen())
        database.drop();
    }

    // Regression: #10961
    @Test
    void aTableDependsOnItsSchema() {
      final int schemaOid = number(resolve("select oid from pg_namespace where nspname = '" + database.getName() + "'").rows.getFirst(), "oid");
      for (final String table : List.of("Account", "Plain")) {
        final List<Map<String, Object>> rows = resolve(
            "select classid, objsubid, refclassid, refobjid, refobjsubid, deptype from pg_depend where classid = " + PG_CLASS
                + " and refclassid = " + PG_NAMESPACE + " and objid = " + oidOfClass(table)).rows;
        assertThat(rows).as(table).hasSize(1);
        assertThat(rows.getFirst()).containsEntry("objsubid", 0).containsEntry("refobjid", schemaOid).containsEntry("refobjsubid", 0)
            .containsEntry("deptype", "n");
      }
    }

    // Regression: #10961
    @Test
    void theRowTypeOfATableDependsOnTheTable() {
      for (final String table : List.of("Account", "Plain")) {
        final int rowType = number(resolve("select reltype from pg_class where relname = '" + table + "'").rows.getFirst(), "reltype");
        final List<Map<String, Object>> rows = resolve(
            "select classid, objsubid, refclassid, refobjid, refobjsubid, deptype from pg_depend where objid = " + rowType).rows;
        assertThat(rows).as(table).hasSize(1);
        assertThat(rows.getFirst()).containsEntry("classid", PG_TYPE).containsEntry("objsubid", 0).containsEntry("refclassid", PG_CLASS)
            .containsEntry("refobjid", oidOfClass(table)).containsEntry("refobjsubid", 0).containsEntry("deptype", "i");
      }
    }

    // Regression: #10961
    /** The walk a tool makes from a schema: every table in it, and from each table its row type, joined to the catalogs by name. */
    @Test
    void walkingFromTheSchemaReachesTheTablesAndTheirRowTypes() {
      final List<Map<String, Object>> tables = resolve("""
          select c.relname from pg_depend d join pg_class c on c.oid = d.objid join pg_namespace n on n.oid = d.refobjid
          where d.classid = 'pg_class'::regclass and d.refclassid = 'pg_namespace'::regclass and d.deptype = 'n'
          and n.nspname = '%s' order by c.relname""".formatted(database.getName())).rows;
      assertThat(tables).extracting(row -> row.get("relname")).containsExactly("Account", "Plain");

      final List<Map<String, Object>> types = resolve("""
          select t.typname from pg_depend d join pg_type t on t.oid = d.objid join pg_class c on c.oid = d.refobjid
          where d.classid = 'pg_type'::regclass and d.refclassid = 'pg_class'::regclass and d.deptype = 'i'
          order by t.typname""").rows;
      assertThat(types).extracting(row -> row.get("typname")).containsExactly("Account", "Plain");
    }

    // Regression: #10961
    @Test
    void theCatalogsOfTheNewDependenciesAreRegclassNames() {
      final Map<String, Object> row = resolve("""
          select 'pg_namespace'::regclass as n, 'pg_catalog.pg_type'::regclass as t, 2615::regclass as nn, 1247::regclass as tn
          from pg_namespace limit 1""").rows.getFirst();
      assertThat(row).containsEntry("n", PG_NAMESPACE).containsEntry("t", PG_TYPE).containsEntry("nn", "pg_namespace")
          .containsEntry("tn", "pg_type");
    }

    private int oidOfClass(final String name) {
      final List<Map<String, Object>> rows = resolve("select oid from pg_class where relname = '" + name + "'").rows;
      assertThat(rows).as(name).hasSize(1);
      return number(rows.getFirst(), "oid");
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
   * Issue #10976: a materialized view is answered by its {@code _RETURN} rewrite rule in PostgreSQL, and {@code pg_depend} lists that
   * rule as part of the view ({@code 'i'}) and as depending on every relation the view reads ({@code 'n'}). This catalog had no
   * {@code pg_rewrite} rows, so a tool walking from a table to what is built on it ({@code pg_depend} joined to {@code pg_rewrite} on
   * the rule, then to {@code pg_class} on {@code ev_class}) found no view. It now lists the rule of each materialized view and
   * continuous aggregate, {@code pg_class.relhasrules} says so, and {@code pg_get_ruledef} prints it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RewriteRuleDepend {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogDependTest-RewriteRuleDepend";
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

    // Regression: #10976
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

    // Regression: #10976
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

    // Regression: #10976
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

    // Regression: #10976
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

    // Regression: #10976
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

  /**
   * Issue #10711 and the two issues it groups: a reference column of the emulated catalog must name a row the catalog has, and a
   * relation has one row set whichever shape of statement reads it.
   * <ul>
   * <li>#10592: six scalar types named an array type ({@code typarray}) that {@code pg_type} had no row for, so a client resolving
   * {@code varchar[]} - pgjdbc's {@code createArrayOf("varchar", ...)} - found nothing.</li>
   * <li>#10693 (refiled as #10745): {@code pg_namespace} read on its own had no row for {@code pg_catalog} (OID 11), the
   * {@code typnamespace} of every type, while the same relation read through a join had it; {@code pg_am} had the same split.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CatalogReferences {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogDependTest-CatalogReferences";
    private static final String USER          = "root";

    private Database database;

    @BeforeEach
    void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();

      database = factory.create();
      // one of everything a family of the catalog has rows for: a type with a super type, a declared default, a unique index and a
      // materialized view
      database.transaction(() -> {
        final Schema schema = database.getSchema();
        final VertexType person = schema.createVertexType("Person");
        person.createProperty("name", Type.STRING);
        person.createProperty("age", Type.INTEGER).setDefaultValue("0");
        schema.createVertexType("Employee").addSuperType("Person").createProperty("salary", Type.DOUBLE);
        schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "name");
      });
      database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a', age = 1"));
      database.command("sql", "CREATE MATERIALIZED VIEW PersonView AS SELECT name FROM Person");
    }

    @AfterEach
    void dropDatabase() {
      if (database != null && database.isOpen())
        database.drop();
    }

    // ---------------------------------------------------------------- #10592: typarray and typelem resolve

    // Regression: #10711
    /** The invariant the report states, over the rows a client reads: every array and element OID pg_type names is a row of it. */
    @Test
    void everyTypeReferenceOfPgTypeIsARowOfPgType() {
      final List<Map<String, Object>> rows = resolve("select oid, typname, typelem, typarray, typinput from pg_catalog.pg_type").rows;
      final Map<Integer, Map<String, Object>> byOid = new HashMap<>();
      for (final Map<String, Object> row : rows)
        byOid.put(number(row, "oid"), row);

      for (final Map<String, Object> row : rows) {
        final int oid = number(row, "oid");
        final int array = number(row, "typarray");
        final int element = number(row, "typelem");
        if (array != 0) {
          assertThat(byOid).as("the array type of %s", row.get("typname")).containsKey(array);
          assertThat(number(byOid.get(array), "typelem")).as("the element of the array type of %s", row.get("typname")).isEqualTo(oid);
        }
        if (element != 0)
          assertThat(byOid).as("the element type of %s", row.get("typname")).containsKey(element);
        // only an array is its element's typarray: name and int2vector name an element too, whose array is "char"[] and int2[]
        // (issue #12449)
        if (element != 0 && "array_in".equals(row.get("typinput"))) {
          assertThat(number(byOid.get(element), "typarray")).as("the array type of the element of %s", row.get("typname"))
              .isEqualTo(oid);
        }
      }
    }

    // Regression: #10711
    /** The report's own sequence, the one a JDBC driver runs for {@code createArrayOf("varchar", ...)}. */
    @Test
    void theArrayTypeOfVarcharIsAType() {
      assertThat(column(resolve("select t.typarray from pg_catalog.pg_type t where t.typname = 'varchar'"), "typarray"))
          .containsExactly(1015);
      assertThat(column(resolve("select typtype, typname, pg_type.oid from pg_catalog.pg_type where oid = 1015"), "typname"))
          .containsExactly("_varchar");
      assertThat(column(resolve("select oid from pg_catalog.pg_type where typname = '_varchar'"), "oid")).containsExactly(1015);
      assertThat(column(resolve("select pg_catalog.format_type(1015, -1) as t from pg_type where oid = 1015"), "t"))
          .containsExactly("character varying[]");
    }

    // Regression: #10711
    @Test
    void theSixMissingArrayTypesAreRows() {
      final PostgresCatalog.Answer answer = resolve(
          "select oid, typname, typelem from pg_catalog.pg_type where oid in (1001, 1005, 1014, 1015, 1182, 1115) order by oid");

      assertThat(column(answer, "typname")).containsExactly("_bytea", "_int2", "_bpchar", "_varchar", "_timestamp", "_date");
      assertThat(column(answer, "typelem")).containsExactly(17, 21, 1042, 1043, 1114, 1082);
    }

    // Regression: #10711
    /** A client that binds a parameter of one of the six array types sends its OID: the protocol parses it rather than refusing. */
    @Test
    void aParameterOfTheNewArrayTypesIsParsed() {
      assertThat(PostgresType.deserialize(1015, 0, "{a,\"b c\"}".getBytes())).isEqualTo(List.of("a", "b c"));
      assertThat(PostgresType.deserialize(1005, 0, "{1,2}".getBytes())).isEqualTo(List.of((short) 1, (short) 2));
      assertThat(PostgresType.deserialize(1014, 0, "{x}".getBytes())).isEqualTo(List.of("x"));
      assertThat((List<?>) PostgresType.deserialize(1182, 0, "{2024-01-02}".getBytes())).hasSize(1);
      assertThat((List<?>) PostgresType.deserialize(1115, 0, "{\"2024-01-02 03:04:05\"}".getBytes())).hasSize(1);
      assertThat((List<?>) PostgresType.deserialize(1001, 0, "{\"\\\\x0102\"}".getBytes())).hasSize(1);
    }

    // ---------------------------------------------------------------- #10745: one row set per relation

    // Regression: #10711
    /** The report's repro: the namespace every type names is a row of pg_namespace when pg_namespace is read on its own. */
    @Test
    void theNamespaceOfEveryTypeIsARowOfPgNamespace() {
      assertThat(column(resolve("select typname, typnamespace from pg_type where typname in ('int4', 'varchar')"), "typnamespace"))
          .containsOnly(11);

      final PostgresCatalog.Answer namespaces = resolve("select oid, nspname from pg_namespace order by oid");
      assertThat(column(namespaces, "oid")).containsExactly(11, 2200);
      assertThat(column(namespaces, "nspname")).containsExactly("pg_catalog", database.getName());

      assertThat(column(resolve("select oid, nspname from pg_namespace where oid = 11"), "nspname")).containsExactly("pg_catalog");
      assertThat(column(resolve("select count(*) from pg_namespace"), "count")).containsExactly(2L);

      assertThat(column(resolve("select t.typname, n.nspname from pg_type t, pg_namespace n where t.typnamespace = n.oid and "
          + "t.typname = 'int4'"), "nspname")).containsExactly("pg_catalog");
      assertThat(column(resolve("select distinct n.nspname, n.oid from pg_namespace n, pg_index i where i.indrelid > 0 order by 2"),
          "oid")).containsExactly(11, 2200);
    }

    // Regression: #10711
    /** The user's relations are still all in the one schema, and a schema browser filtering out the catalog sees only that. */
    @Test
    void theUsersRelationsStayInTheirOneSchema() {
      assertThat(column(resolve("select distinct n.nspname from pg_class c join pg_namespace n on n.oid = c.relnamespace"), "nspname"))
          .containsExactly(database.getName());
      // psql's \dn
      assertThat(column(resolve("""
          SELECT n.nspname AS "Name", pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
          FROM pg_catalog.pg_namespace n
          WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
          ORDER BY 1;"""), "Name")).containsExactly(database.getName());
    }

    // Regression: #10711
    /**
     * Found with the two-row pg_namespace: an ORDER BY naming a column the projection carries under another name - DbVisualizer's
     * schema list - left the rows in the order they were built, as it did any table list ordered that way.
     */
    @Test
    void anOrderByAColumnProjectedUnderAnotherNameSorts() {
      assertThat(column(resolve("select nspname as schema_name from pg_namespace order by NSPNAME desc"), "schema_name"))
          .containsExactly("pg_catalog", database.getName());
      assertThat(column(resolve("select nspname as schema_name from pg_namespace order by nspname asc"), "schema_name"))
          .containsExactly(database.getName(), "pg_catalog");
      assertThat(column(resolve("select c.relname as r from pg_class c where c.relkind = 'r' order by c.relname desc"), "r"))
          .containsExactly("Person", "Employee");
      assertThat(column(resolve("select relname as r from pg_class where relkind = 'r' order by relname desc"), "r"))
          .containsExactly("Person", "Employee");
    }

    // Regression: #10711
    /** {@code pg_am} on its own: the heap every type is stored in and the index families, as a join reads it. */
    @Test
    void pgAmIsTheSameRelationOnItsOwnAndInAJoin() {
      final PostgresCatalog.Answer alone = resolve("select oid, amname, amtype from pg_am order by oid");
      assertThat(column(alone, "amname")).containsExactly("heap", "btree", "hash", "gist", "gin", "brin", "hnsw");
      assertThat(column(alone, "amtype")).containsExactly("t", "i", "i", "i", "i", "i", "i");

      // \dt joins it on relam, and the table's access method is the heap
      assertThat(column(resolve("select am.amname from pg_class c left join pg_am am on am.oid = c.relam where c.relname = 'Person'"),
          "amname")).containsExactly("heap");
    }

    // Regression: #10711
    /** The default collation every text type names in typcollation is a row of pg_collation, and so is the C of name (#12449). */
    @Test
    void theCollationOfTheTextTypesIsARowOfPgCollation() {
      assertThat(column(resolve("select distinct typcollation from pg_type where typcollation <> 0"), "typcollation"))
          .containsExactlyInAnyOrder(100, 950);
      final PostgresCatalog.Answer collations = resolve("select oid, collname, collnamespace from pg_collation");
      assertThat(column(collations, "oid")).containsExactly(100, 950);
      assertThat(column(collations, "collname")).containsExactly("default", "C");
      assertThat(column(collations, "collnamespace")).containsExactly(11, 11);
    }

    // Regression: #10711
    /**
     * The guarantee the report asks for: for every relation this catalog models, the rows a join reads are the rows the relation has
     * when it is read on its own. A join may filter, it may not conjure; and a relation that is refused on its own has nothing a join
     * can read either.
     */
    @Test
    void everyRelationHasTheSameRowsReadAloneAndThroughAJoin() {
      final Set<String> checked = new HashSet<>();
      for (final String relation : PostgresCatalog.modelledRelations()) {
        final PostgresCatalog.Answer alone = PostgresCatalog.resolve("select * from " + relation, database, USER);
        // the join path: a table function next to it makes the statement a join of the rows of each entry
        final PostgresCatalog.Answer joined = PostgresCatalog.resolve(
            "select x.* from " + relation + " x, generate_series(1, 1) g", database, USER);

        assertThat(joined).as("answered: %s through a join", relation).isNotNull();
        assertThat(joined.rows).as("answered: %s through a join", relation).isNotNull();
        if (alone == null || alone.rows == null) {
          assertThat(joined.rows).as("%s is refused on its own, so a join has no row of it", relation).isEmpty();
          continue;
        }
        assertThat(normalized(joined.rows)).as("the rows of %s through a join", relation)
            .containsExactlyInAnyOrderElementsOf(normalized(alone.rows));
        checked.add(relation);
      }
      assertThat(checked).contains("pg_namespace", "pg_am", "pg_type", "pg_class", "pg_attribute", "pg_index", "pg_collation");
    }

    // ---------------------------------------------------------------- helpers

    private PostgresCatalog.Answer resolve(final String query) {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
      assertThat(answer).as("answered: %s", query).isNotNull();
      assertThat(answer.rows).as("answered rather than declined: %s", query).isNotNull();
      return answer;
    }

    /** Rows compared by value, column names folded and vectors read as text, whatever map each path builds them in. */
    private static List<Map<String, Object>> normalized(final List<Map<String, Object>> rows) {
      final List<Map<String, Object>> result = new ArrayList<>(rows.size());
      for (final Map<String, Object> row : rows) {
        final Map<String, Object> copy = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (final Map.Entry<String, Object> entry : row.entrySet())
          copy.put(entry.getKey(), entry.getValue() instanceof List<?> list ? list.toString() : entry.getValue());
        result.add(copy);
      }
      return result;
    }

    private static int number(final Map<String, Object> row, final String column) {
      return ((Number) row.get(column)).intValue();
    }

    private static List<Object> column(final PostgresCatalog.Answer answer, final String name) {
      final List<Object> values = new ArrayList<>();
      for (final Map<String, Object> row : answer.rows)
        values.add(row.get(name));
      return values;
    }
  }

  /**
   * Issue #12449: the reference columns of {@code pg_type} answered 0 where PostgreSQL 16 names an array type ({@code typarray}), an
   * element type ({@code typelem}) or a collation ({@code typcollation}) the catalog did not model. The reference values below were read
   * from a stock {@code postgres:16} with {@code SELECT oid, typname, typelem, typarray, typcollation, typlen, typbyval, typcategory,
   * typstorage, typinput, typreceive FROM pg_type}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PgTypeReferences {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogDependTest-PgTypeReferences";
    private static final String USER          = "root";

    /** oid, typname, typelem, typarray, typcollation, typlen, typbyval, typcategory, typstorage, typinput, typreceive. */
    private static final Object[][] POSTGRESQL_16 = { //
        { 19, "name", 18, 1003, 950, 64, false, "S", "p", "namein", "namerecv" },//
        { 22, "int2vector", 21, 1006, 0, -1, false, "A", "p", "int2vectorin", "int2vectorrecv" },//
        { 26, "oid", 0, 1028, 0, 4, true, "N", "p", "oidin", "oidrecv" },//
        { 1083, "time", 0, 1183, 0, 8, true, "D", "p", "time_in", "time_recv" },//
        { 1184, "timestamptz", 0, 1185, 0, 8, true, "D", "p", "timestamptz_in", "timestamptz_recv" },//
        { 1186, "interval", 0, 1187, 0, 16, false, "T", "p", "interval_in", "interval_recv" },//
        { 1266, "timetz", 0, 1270, 0, 12, false, "D", "p", "timetz_in", "timetz_recv" },//
        { 3802, "jsonb", 0, 3807, 0, -1, false, "U", "x", "jsonb_in", "jsonb_recv" },//
        { 1003, "_name", 19, 0, 950, -1, false, "A", "x", "array_in", "array_recv" },//
        { 1006, "_int2vector", 22, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
        { 1028, "_oid", 26, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
        { 1183, "_time", 1083, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
        { 1185, "_timestamptz", 1184, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
        { 1187, "_interval", 1186, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
        { 1270, "_timetz", 1266, 0, 0, -1, false, "A", "x", "array_in", "array_recv" },//
        { 3807, "_jsonb", 3802, 0, 0, -1, false, "A", "x", "array_in", "array_recv" } };

    /** The columns of {@link #POSTGRESQL_16}, in order. */
    private static final String[] COLUMNS = { "oid", "typname", "typelem", "typarray", "typcollation", "typlen", "typbyval",
        "typcategory", "typstorage", "typinput", "typreceive" };
    /** oid, typname and the three reference columns the issue is about. */
    private static final int      REFERENCE_COLUMNS = 5;

    private Database database;

    @BeforeEach
    void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void dropDatabase() {
      if (database != null && database.isOpen())
        database.drop();
    }

    // Regression: #12449
    @Test
    void theRowsTheIssueNamesAnswerWhatPostgreSQL16Answers() {
      final String projection = String.join(", ", COLUMNS);
      for (final Object[] expected : POSTGRESQL_16) {
        // both routes a client reaches pg_type by: the recogniser and the generic catalog
        final List<Map<String, Object>> recognised = PostgresTypeCatalog.resolve(
            "SELECT " + projection + " FROM pg_type WHERE oid = " + expected[0]);
        final List<Map<String, Object>> generic = resolve("SELECT " + projection + " FROM pg_catalog.pg_type WHERE typname = '" + expected[1]
            + "'").rows;
        // every column of the array types this issue adds; the reference columns of the scalars, whose attribute columns are #12313's
        final int columns = ((String) expected[1]).startsWith("_") ? COLUMNS.length : REFERENCE_COLUMNS;
        for (final List<Map<String, Object>> rows : List.of(recognised, generic)) {
          assertThat(rows).as("the row of %s", expected[1]).hasSize(1);
          for (int i = 0; i < columns; i++)
            assertThat(normalized(rows.getFirst().get(COLUMNS[i]))).as("%s.%s", expected[1], COLUMNS[i]).isEqualTo(normalized(expected[i]));
        }
      }
    }

    // Regression: #12449
    @Test
    void theCollationOfNameIsARowOfPgCollation() {
      final PostgresCatalog.Answer collations = resolve("select oid, collname, collprovider, collcollate from pg_collation");
      assertThat(column(collations, "oid")).containsExactly(100, 950);
      assertThat(column(collations, "collname")).containsExactly("default", "C");
      assertThat(column(collations, "collcollate")).containsExactly(null, "C");

      // a join on typcollation reads the collation the type names
      assertThat(column(resolve("select t.typname, c.collname from pg_catalog.pg_type t join pg_catalog.pg_collation c "
          + "on c.oid = t.typcollation where t.typname in ('name', 'text', '_name')"), "collname")).containsExactlyInAnyOrder("C",
          "default", "C");
    }

    // Regression: #12449
    @Test
    void theElementJoinFollowsTypelem() {
      // the self-join a driver resolves an array's element with, now also for the arrays #12449 adds, and for the typelem of name
      assertThat(PostgresTypeCatalog.resolve("SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 1183 AND t.typelem = e.oid"))
          .containsExactly(Map.of("typname", "time"));
      assertThat(PostgresTypeCatalog.resolve("SELECT e.typname FROM pg_type t, pg_type e WHERE t.oid = 19 AND t.typelem = e.oid"))
          .containsExactly(Map.of("typname", "char"));
    }

    // Regression: #12449
    @Test
    void nameAndInt2vectorAreNotArraysOfThisProtocol() {
      assertThat(PostgresType.NAME.isArrayType()).isFalse();
      assertThat(PostgresType.INT2VECTOR.isArrayType()).isFalse();
      assertThat(PostgresType.ARRAY_NAME.isArrayType()).isTrue();
      assertThat(PostgresType.ARRAY_NAME.sqlName()).isEqualTo("name[]");
      assertThat(PostgresType.ARRAY_TIMESTAMPTZ.sqlName()).isEqualTo("timestamp with time zone[]");
    }

    // Regression: #12449
    @Test
    void theNewArrayTypesAreReadAsParameters() {
      assertThat(PostgresType.deserialize(1183, 0, bytes("{12:00:00,13:30:15}"))).isEqualTo(List.of(LocalTime.of(12, 0),
          LocalTime.of(13, 30, 15)));
      assertThat(PostgresType.deserialize(1028, 0, bytes("{1,4294967295}"))).isEqualTo(List.of(1L, 4294967295L));
      assertThat(PostgresType.deserialize(1003, 0, bytes("{a,b}"))).isEqualTo(List.of("a", "b"));

      // binary: time[] of two elements, microseconds since midnight
      final ByteBuffer times = arrayHeader(2, 1083);
      times.putInt(8).putLong(43_200_000_000L).putInt(8).putLong(48_615_000_000L);
      assertThat(PostgresType.deserialize(1183, 1, times.array())).isEqualTo(List.of(LocalTime.of(12, 0), LocalTime.of(13, 30, 15)));

      // binary: oid[] holds unsigned 32-bit numbers
      final ByteBuffer oids = arrayHeader(2, 26);
      oids.putInt(4).putInt(7).putInt(4).putInt(-1);
      assertThat(PostgresType.deserialize(1028, 1, oids.array())).isEqualTo(List.of(7L, 4294967295L));

      // binary: timetz[] of one element, the offset in seconds WEST of UTC
      final ByteBuffer timetz = arrayHeader(1, 1266);
      timetz.putInt(12).putLong(3_600_000_000L).putInt(-7200);
      assertThat(PostgresType.deserialize(1270, 1, timetz.array())).isEqualTo(List.of(OffsetTime.of(LocalTime.of(1, 0),
          ZoneOffset.ofHours(2))));

      // binary: interval[] of one element: microseconds, days, months
      final ByteBuffer interval = arrayHeader(1, 1186);
      interval.putInt(16).putLong(1_500_000L).putInt(2).putInt(3);
      assertThat(PostgresType.deserialize(1187, 1, interval.array())).isEqualTo(List.of(new CypherDuration(3, 2, 1, 500_000_000)));

      // binary: jsonb[] elements carry jsonb_send's version byte
      final byte[] json = bytes("{\"a\":1}");
      final ByteBuffer jsonb = arrayHeader(1, 3802);
      jsonb.putInt(json.length + 1).put((byte) 1).put(json);
      final List<?> documents = (List<?>) PostgresType.deserialize(3807, 1, jsonb.array());
      assertThat(documents).hasSize(1);
      assertThat(documents.getFirst().toString()).contains("\"a\"").contains("1");
    }

    // Regression: #12449
    @Test
    void theScalarsOfTheNewArraysAreReadInBinary() {
      assertThat(PostgresType.deserialize(26, 1, ByteBuffer.allocate(4).putInt(-1).array())).isEqualTo(4294967295L);
      assertThat(PostgresType.deserialize(19, 1, bytes("pg_class"))).isEqualTo("pg_class");

      // int2vectorsend is the array format of int2[], and the text format reads "1 3"
      final ByteBuffer vector = arrayHeader(2, 21);
      vector.putInt(2).putShort((short) 1).putInt(2).putShort((short) 3);
      assertThat(PostgresType.deserialize(22, 1, vector.array())).isEqualTo("1 3");
      assertThat(PostgresType.deserialize(22, 0, bytes("1 3"))).isEqualTo("1 3");
    }

    /** A one-dimensional binary array of {@code count} elements of {@code elementOid}, with room for the elements. */
    private static ByteBuffer arrayHeader(final int count, final int elementOid) {
      final ByteBuffer buffer = ByteBuffer.allocate(20 + count * 24);
      buffer.putInt(1).putInt(0).putInt(elementOid).putInt(count).putInt(1);
      return buffer;
    }

    private static byte[] bytes(final String text) {
      return text.getBytes(StandardCharsets.UTF_8);
    }

    private static Object normalized(final Object value) {
      return value instanceof Number number ? (Object) number.longValue() : value;
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
}
