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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The columns the emulated PostgreSQL catalog describes: {@code information_schema.columns}, {@code pg_attribute} and the types of
 * catalog answers, inherited columns, the row type of each relation, and the columns and relation kind of materialized views.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresCatalogColumnsTest {

  /**
   * Issue #10370: {@code information_schema.columns.data_type} answered the internal type name ({@code int4}, {@code varchar},
   * {@code _text}) where PostgreSQL answers the SQL standard name ({@code integer}, {@code character varying}, {@code ARRAY}). The
   * same row contradicted itself: {@code format_type()} and {@code ::regtype} read {@code integer} for the same column, and
   * {@code data_type} and {@code udt_name} were always equal, which PostgreSQL deliberately keeps apart. The precision columns the
   * relation declares were never written either.
   * <p>
   * The SQL name now lives in one place, {@link PostgresType#sqlName()}, which {@code format_type}, {@code regtype} and the
   * {@code information_schema} row all read, and one test drives every ArcadeDB type through both surfaces so a new type cannot
   * disagree with itself.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class InformationSchemaDataType {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogColumnsTest-InformationSchemaDataType";
    private static final String USER          = "root";

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

    // Regression: #10370
    /** The reproduction of the issue: one column of each shape, read through information_schema. */
    @Test
    void dataTypeIsTheSqlNameAndUdtNameIsTheInternalOne() {
      database.transaction(() -> {
        final DocumentType person = database.getSchema().createVertexType("Person");
        person.createProperty("age", Type.INTEGER);
        person.createProperty("name", Type.STRING);
        person.createProperty("born", Type.DATETIME);
        person.createProperty("score", Type.DOUBLE);
        person.createProperty("ok", Type.BOOLEAN);
        person.createProperty("tags", Type.LIST, "STRING");
      });

      final Map<String, Map<String, Object>> columns = byColumn(resolve(
          "select column_name, data_type, udt_name from information_schema.columns where table_name = 'Person' order by ordinal_position"));

      assertThat(columns.get("age")).containsEntry("data_type", "integer").containsEntry("udt_name", "int4");
      assertThat(columns.get("name")).containsEntry("data_type", "character varying").containsEntry("udt_name", "varchar");
      assertThat(columns.get("born")).containsEntry("data_type", "timestamp without time zone").containsEntry("udt_name", "timestamp");
      assertThat(columns.get("score")).containsEntry("data_type", "double precision").containsEntry("udt_name", "float8");
      assertThat(columns.get("ok")).containsEntry("data_type", "boolean").containsEntry("udt_name", "bool");
      // PostgreSQL puts the literal ARRAY in data_type for any array column: the element type is in element_types, and "_text" is
      // not a value data_type can ever hold
      assertThat(columns.get("tags")).containsEntry("data_type", "ARRAY").containsEntry("udt_name", "_text");
    }

    // Regression: #10370
    /**
     * The guarantee that stops the family coming back: for a column of EVERY ArcadeDB type, {@code format_type()} and
     * {@code data_type} name the same type, except that an array is {@code ARRAY} in the second and {@code <element>[]} in the first.
     */
    @Test
    void everyTypeIsNamedTheSameByFormatTypeAndByTheInformationSchema() {
      database.transaction(() -> database.getSchema().createDocumentType("AllTypes"));
      final DocumentType type = database.getSchema().getType("AllTypes");
      final List<Type> created = new ArrayList<>();
      for (final Type arcadeType : Type.values()) {
        try {
          database.transaction(() -> type.createProperty("p_" + arcadeType.name(), arcadeType));
          created.add(arcadeType);
        } catch (final RuntimeException e) {
          // a type a property cannot be declared with on its own has no column to compare
        }
      }
      assertThat(created).as("columns compared").hasSizeGreaterThan(15);

      final Map<String, Map<String, Object>> formatted = byColumn(resolve(
          "select a.attname as column_name, pg_catalog.format_type(a.atttypid, a.atttypmod) as ft, a.atttypid::pg_catalog.regtype as rt "
              + "from pg_catalog.pg_attribute a, pg_catalog.pg_class c where a.attrelid = c.oid and c.relname = 'AllTypes' "
              + "order by a.attnum"));
      final Map<String, Map<String, Object>> information = byColumn(resolve(
          "select column_name, data_type, udt_name from information_schema.columns where table_name = 'AllTypes' order by ordinal_position"));

      assertThat(information.keySet()).containsExactlyInAnyOrderElementsOf(formatted.keySet());
      for (final String column : information.keySet()) {
        final String formatType = (String) formatted.get(column).get("ft");
        final String dataType = (String) information.get(column).get("data_type");
        final String udtName = (String) information.get(column).get("udt_name");

        assertThat(formatted.get(column).get("rt")).as("regtype of %s", column).isEqualTo(formatType);
        if (formatType.endsWith("[]")) {
          assertThat(dataType).as("data_type of the array column %s", column).isEqualTo("ARRAY");
          assertThat(udtName).as("udt_name of the array column %s", column).startsWith("_");
        } else {
          assertThat(dataType).as("data_type of %s must read what format_type() reads", column).isEqualTo(formatType);
          // udt_name is the pg_type name, so it differs from the SQL name exactly where PostgreSQL's does
          assertThat(udtName).as("udt_name of %s", column).isNotNull();
        }
      }
    }

    // Regression: #10370
    /** {@code sqlName()} is the single accessor: every constant answers a name, and an array answers its element's, bracketed. */
    @Test
    void everyPostgresTypeHasOneSqlName() {
      for (final PostgresType type : PostgresType.values()) {
        assertThat(type.sqlName()).as("sqlName of %s", type).isNotBlank();
        if (type.isArrayType()) {
          final PostgresType element = PostgresType.byCode(type.elementCode);
          assertThat(type.sqlName()).as("sqlName of %s", type).isEqualTo(element.sqlName() + "[]");
        }
      }
      assertThat(PostgresType.INTEGER.sqlName()).isEqualTo("integer");
      assertThat(PostgresType.VARCHAR.sqlName()).isEqualTo("character varying");
      assertThat(PostgresType.ARRAY_TEXT.sqlName()).isEqualTo("text[]");
      assertThat(PostgresType.INTEGER.typeName).as("the pg_type name is a different thing and stays").isEqualTo("int4");
    }

    // Regression: #10370
    /** The precision columns the relation declares, written for the built-in types that have them (typmod -1: none declared). */
    @Test
    void thePrecisionColumnsAreWrittenAsPostgreSqlWritesThem() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("Precise");
        type.createProperty("i", Type.INTEGER);
        type.createProperty("s", Type.SHORT);
        type.createProperty("l", Type.LONG);
        type.createProperty("f", Type.FLOAT);
        type.createProperty("d", Type.DOUBLE);
        type.createProperty("n", Type.DECIMAL);
        type.createProperty("t", Type.DATETIME);
        type.createProperty("dt", Type.DATE);
        type.createProperty("v", Type.STRING);
        type.createProperty("b", Type.BOOLEAN);
      });

      final Map<String, Map<String, Object>> columns = byColumn(resolve(
          "select column_name, character_maximum_length, character_octet_length, numeric_precision, numeric_precision_radix, "
              + "numeric_scale, datetime_precision from information_schema.columns where table_name = 'Precise' order by ordinal_position"));

      assertNumbers(columns.get("i"), null, null, 32, 2, 0, null);
      assertNumbers(columns.get("s"), null, null, 16, 2, 0, null);
      assertNumbers(columns.get("l"), null, null, 64, 2, 0, null);
      assertNumbers(columns.get("f"), null, null, 24, 2, null, null);
      assertNumbers(columns.get("d"), null, null, 53, 2, null, null);
      // NUMERIC with no declared precision has none to report, and is the one type counted in base 10
      assertNumbers(columns.get("n"), null, null, null, 10, null, null);
      assertNumbers(columns.get("t"), null, null, null, null, null, 6);
      assertNumbers(columns.get("dt"), null, null, null, null, null, 0);
      // a varchar with no declared length has no maximum, and PostgreSQL's octet length for it is 2^30
      assertNumbers(columns.get("v"), null, 1_073_741_824, null, null, null, null);
      assertNumbers(columns.get("b"), null, null, null, null, null, null);
    }

    private static void assertNumbers(final Map<String, Object> column, final Integer characterMaximumLength,
        final Integer characterOctetLength, final Integer numericPrecision, final Integer radix, final Integer numericScale,
        final Integer datetimePrecision) {
      assertThat(column).as("column row").isNotNull();
      assertThat(column.get("character_maximum_length")).as("character_maximum_length of %s", column.get("column_name"))
          .isEqualTo(characterMaximumLength);
      assertThat(column.get("character_octet_length")).as("character_octet_length of %s", column.get("column_name"))
          .isEqualTo(characterOctetLength);
      assertThat(column.get("numeric_precision")).as("numeric_precision of %s", column.get("column_name")).isEqualTo(numericPrecision);
      assertThat(column.get("numeric_precision_radix")).as("numeric_precision_radix of %s", column.get("column_name")).isEqualTo(radix);
      assertThat(column.get("numeric_scale")).as("numeric_scale of %s", column.get("column_name")).isEqualTo(numericScale);
      assertThat(column.get("datetime_precision")).as("datetime_precision of %s", column.get("column_name")).isEqualTo(datetimePrecision);
    }

    private List<Map<String, Object>> resolve(final String query) {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
      assertThat(answer).as("no answer: %s", query).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
      return answer.rows;
    }

    private static Map<String, Map<String, Object>> byColumn(final List<Map<String, Object>> rows) {
      final Map<String, Map<String, Object>> byName = new HashMap<>();
      for (final Map<String, Object> row : rows)
        byName.put((String) row.get("column_name"), row);
      return byName;
    }
  }

  /**
   * Issue #10594: {@code pg_class.relnatts} counted the properties a type declares itself, hidden ones included, while
   * {@code pg_attribute} and {@code information_schema.columns} list its visible properties, inherited ones included; and every
   * column was reported as locally declared ({@code attislocal = true, attinhcount = 0}) although {@code pg_inherits} names the
   * type's parents. PostgreSQL keeps {@code relnatts} equal to the number of {@code pg_attribute} rows of a relation, and marks a
   * column a relation did not declare itself with {@code attislocal = false} and {@code attinhcount} = the number of its direct
   * parents that carry it.
   */
  @Nested
  class InheritedColumns {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogColumnsTest-InheritedColumns";
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
        // the issue's hierarchy: Super(sa, sb) <- Sub(own) <- SubSub(deep)
        schema.createDocumentType("Super").createProperty("sa", Type.STRING);
        schema.getType("Super").createProperty("sb", Type.INTEGER);
        schema.createDocumentType("Sub").addSuperType("Super").createProperty("own", Type.STRING);
        schema.createDocumentType("SubSub").addSuperType("Sub").createProperty("deep", Type.STRING);

        // a diamond: Bottom reaches Base's x through two direct parents
        schema.createDocumentType("Base").createProperty("x", Type.STRING);
        schema.createDocumentType("Left").addSuperType("Base").createProperty("l", Type.STRING);
        schema.createDocumentType("Right").addSuperType("Base").createProperty("r", Type.STRING);
        schema.createDocumentType("Bottom").addSuperType("Left").addSuperType("Right");

        // the correction posted on the issue: a flat type with a hidden property
        schema.createDocumentType("Flat").createProperty("visible", Type.STRING);
        schema.getType("Flat").createProperty("secret", Type.STRING);
        schema.getType("Flat").getProperty("secret").setHidden(true);

        // a hidden property of a parent is a column of neither the parent nor its children
        schema.createDocumentType("HiddenParent").createProperty("shown", Type.STRING);
        schema.getType("HiddenParent").createProperty("concealed", Type.STRING);
        schema.getType("HiddenParent").getProperty("concealed").setHidden(true);
        schema.createDocumentType("HiddenChild").addSuperType("HiddenParent").createProperty("mine", Type.STRING);

        // a name held by the type and by a super type added afterwards, which addSuperType only logs: one merged column
        schema.createDocumentType("Merged").createProperty("m", Type.STRING);
        schema.createDocumentType("MergedParent").createProperty("m", Type.STRING);
        schema.getType("Merged").addSuperType("MergedParent");

        // the same, with the parent's declaration hidden: the parent has no column m, so the child's is inherited from nothing
        schema.createDocumentType("Shadowing").createProperty("m", Type.STRING);
        schema.createDocumentType("HiddenMergedParent").createProperty("m", Type.STRING);
        schema.getType("HiddenMergedParent").getProperty("m").setHidden(true);
        schema.getType("Shadowing").addSuperType("HiddenMergedParent");

        schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Super", "sa");
      });
    }

    @AfterEach
    void dropDatabase() {
      if (database != null && database.isOpen())
        database.drop();
    }

    // Regression: #10594
    /** The issue's first query: a subtype's relnatts counts its inherited columns. */
    @Test
    void relnattsOfASubtypeCountsItsInheritedColumns() {
      final Map<String, Integer> relnatts = relnattsByTable();
      assertThat(relnatts.get("Super")).isEqualTo(2);
      assertThat(relnatts.get("Sub")).isEqualTo(3);
      assertThat(relnatts.get("SubSub")).isEqualTo(4);
      assertThat(relnatts.get("Bottom")).isEqualTo(3);
    }

    // Regression: #10594
    /** The correction on the issue: a hidden property is no column, so relnatts does not count it either. */
    @Test
    void relnattsDoesNotCountHiddenProperties() {
      final Map<String, Integer> relnatts = relnattsByTable();
      assertThat(relnatts.get("Flat")).isEqualTo(1);
      assertThat(relnatts.get("HiddenParent")).isEqualTo(1);
      assertThat(relnatts.get("HiddenChild")).isEqualTo(2);
    }

    // Regression: #10594
    /** The issue's second query: a column inherited from the parent is not local and is inherited once. */
    @Test
    void anInheritedColumnIsNotLocal() {
      final PostgresCatalog.Answer answer = resolve("""
          select a.attname, a.attnum, a.attislocal, a.attinhcount
            from pg_catalog.pg_attribute a, pg_catalog.pg_class c
           where c.relname='Sub' and a.attrelid=c.oid and a.attnum>0 order by a.attnum""");

      assertThat(column(answer, "attname")).containsExactly("own", "sa", "sb");
      assertThat(column(answer, "attislocal")).containsExactly(true, false, false);
      assertThat(column(answer, "attinhcount")).containsExactly(0, 1, 1);
    }

    // Regression: #10594
    /** The type that declares a column keeps it local; a grandchild inherits it through its one direct parent. */
    @Test
    void localityFollowsTheDeclaringTypeAndCountsDirectParents() {
      assertThat(attributes("Super")).containsExactly("sa:true:0", "sb:true:0");
      assertThat(attributes("SubSub")).containsExactly("deep:true:0", "own:false:1", "sa:false:1", "sb:false:1");
      // PostgreSQL counts a column merged from two parents once per parent
      assertThat(attributes("Bottom")).containsExactly("l:false:1", "r:false:1", "x:false:2");
      assertThat(attributes("Flat")).containsExactly("visible:true:0");
      assertThat(attributes("HiddenChild")).containsExactly("mine:true:0", "shown:false:1");
    }

    // Regression: #10594
    /** A name both declared by the type and inherited is one column, local and inherited once, as PostgreSQL merges it. */
    @Test
    void aNameDeclaredAndInheritedIsOneMergedColumn() {
      assertThat(attributes("Merged")).containsExactly("m:true:1");
      assertThat(relnattsByTable().get("Merged")).isEqualTo(1);
    }

    // Regression: #10594
    /** A parent whose declaration of the name is hidden lists no such column, so the child's column inherits from no parent. */
    @Test
    void aHiddenParentDeclarationIsNotCountedAsInherited() {
      assertThat(attributes("Shadowing")).containsExactly("m:true:0");
      assertThat(attributes("HiddenMergedParent")).isEmpty();
    }

    // Regression: #10594
    /** An index's key columns are columns of the index relation itself, as in PostgreSQL: local, inherited from nothing. */
    @Test
    void indexKeyColumnsStayLocal() {
      final PostgresCatalog.Answer answer = resolve("""
          select a.attname, a.attislocal, a.attinhcount
            from pg_catalog.pg_attribute a, pg_catalog.pg_class c
           where c.relkind='i' and a.attrelid=c.oid""");
      assertThat(column(answer, "attname")).containsExactly("sa");
      assertThat(column(answer, "attislocal")).containsExactly(true);
      assertThat(column(answer, "attinhcount")).containsExactly(0);
    }

    // Regression: #10594
    /**
     * The invariant PostgreSQL keeps and the issue asks to assert over the whole schema: for every relation of kind {@code r},
     * relnatts equals the number of its pg_attribute rows with attnum > 0, and of its information_schema.columns rows.
     */
    @Test
    void relnattsEqualsTheColumnCountOfEveryTable() {
      final Map<String, Integer> relnatts = relnattsByTable();
      final Map<Integer, String> nameByOid = new HashMap<>();
      for (final Map<String, Object> row : resolve("select oid, relname from pg_class where relkind = 'r'").rows)
        nameByOid.put(((Number) row.get("oid")).intValue(), (String) row.get("relname"));

      final Map<String, Integer> attributeCount = new HashMap<>();
      for (final Map<String, Object> row : resolve("select attrelid, attnum from pg_attribute where attnum > 0").rows) {
        final String table = nameByOid.get(((Number) row.get("attrelid")).intValue());
        if (table != null)
          attributeCount.merge(table, 1, Integer::sum);
      }

      final Map<String, Integer> columnCount = new HashMap<>();
      for (final Map<String, Object> row : resolve("select table_name from information_schema.columns").rows)
        columnCount.merge((String) row.get("table_name"), 1, Integer::sum);

      assertThat(relnatts).isNotEmpty();
      for (final Map.Entry<String, Integer> entry : relnatts.entrySet()) {
        assertThat(attributeCount.getOrDefault(entry.getKey(), 0)).as("pg_attribute rows of %s", entry.getKey())
            .isEqualTo(entry.getValue());
        assertThat(columnCount.getOrDefault(entry.getKey(), 0)).as("information_schema.columns rows of %s", entry.getKey())
            .isEqualTo(entry.getValue());
      }
    }

    // ---------------------------------------------------------------- helpers

    private Map<String, Integer> relnattsByTable() {
      final Map<String, Integer> relnatts = new HashMap<>();
      for (final Map<String, Object> row : resolve("select relname, relnatts from pg_class where relkind = 'r'").rows)
        relnatts.put((String) row.get("relname"), ((Number) row.get("relnatts")).intValue());
      return relnatts;
    }

    private List<String> attributes(final String table) {
      final PostgresCatalog.Answer answer = resolve("""
          select a.attname, a.attislocal, a.attinhcount
            from pg_catalog.pg_attribute a, pg_catalog.pg_class c
           where c.relname='%s' and a.attrelid=c.oid and a.attnum>0 order by a.attnum""".formatted(table));
      final List<String> values = new ArrayList<>();
      for (final Map<String, Object> row : answer.rows)
        values.add(row.get("attname") + ":" + row.get("attislocal") + ":" + row.get("attinhcount"));
      return values;
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
   * Issue #10869: the columns of a catalog answer were typed by sampling the rows that came back, so an answer with no row carried no
   * type at all - a pg_type lookup of a type the server does not have announced ZERO fields for a three-column projection, and every
   * other relation announced every column as varchar where the same projection with a matching filter announced int4 or bool.
   * <p>
   * The census holds every modelled relation to one contract: its columns are typed by the relation, the values its rows carry are of
   * those types, and a filter that matches nothing announces exactly what a filter that matches announces.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ColumnTypes {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogColumnsTest-ColumnTypes";
    private static final String USER          = "root";

    private Database database;

    @BeforeEach
    void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();

      database = factory.create();
      // a schema that gives every relation rows: properties of every kind with defaults and constraints, indexes of every
      // family, an inheritance, a materialized view and a continuous aggregate
      database.command("sqlscript", """
          CREATE DOCUMENT TYPE Probe;
          CREATE PROPERTY Probe.s STRING (mandatory true, notnull true, default 'x');
          CREATE PROPERTY Probe.i INTEGER (default 3);
          CREATE PROPERTY Probe.l LONG;
          CREATE PROPERTY Probe.d DOUBLE;
          CREATE PROPERTY Probe.f FLOAT;
          CREATE PROPERTY Probe.b BOOLEAN;
          CREATE PROPERTY Probe.dt DATETIME;
          CREATE PROPERTY Probe.da DATE;
          CREATE PROPERTY Probe.dec DECIMAL;
          CREATE PROPERTY Probe.bin BINARY;
          CREATE PROPERTY Probe.lst LIST OF STRING;
          CREATE PROPERTY Probe.m MAP;
          CREATE PROPERTY Probe.sh SHORT;
          CREATE PROPERTY Probe.bt BYTE;
          CREATE PROPERTY Probe.txt STRING;
          CREATE INDEX ON Probe (s) UNIQUE;
          CREATE INDEX ON Probe (i, l) NOTUNIQUE;
          CREATE INDEX ON Probe (txt) FULL_TEXT;
          CREATE VERTEX TYPE Parent;
          CREATE PROPERTY Parent.name STRING;
          CREATE VERTEX TYPE Child EXTENDS Parent;
          CREATE EDGE TYPE Knows;
          BEGIN;
          INSERT INTO Probe SET s = 'a', i = 1, l = 2, d = 1.5, f = 2.5, b = true, sh = 1, bt = 2, dec = 1.25;
          COMMIT;
          CREATE MATERIALIZED VIEW ProbeView AS SELECT s, i FROM Probe;
          CREATE TIMESERIES TYPE Metrics TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE);
          """);
      database.command("sql",
          "CREATE CONTINUOUS AGGREGATE MetricsHourly AS SELECT host, avg(v) AS av, ts.timeBucket('1h', ts) AS hour FROM Metrics GROUP BY host, hour");
    }

    @AfterEach
    void dropDatabase() {
      if (database != null && database.isOpen())
        database.drop();
    }

    // Regression: #10869
    @Test
    void everyRelationAnnouncesTheSameColumnsWhetherItsFilterMatchesOrNot() {
      final TreeSet<String> checked = new TreeSet<>();
      for (final String relation : new TreeSet<>(PostgresCatalog.modelledRelations())) {
        final PostgresCatalog.Answer all = PostgresCatalog.resolve("SELECT * FROM " + relation, database, USER);
        final PostgresCatalog.Answer none = PostgresCatalog.resolve("SELECT * FROM " + relation + " WHERE 1 = 0", database, USER);
        if (all == null || all == PostgresCatalog.DECLINED) {
          // a relation that is only ever read joined to another one (pg_description) is not answered on its own either way
          assertThat(none == null || none == PostgresCatalog.DECLINED).as(relation).isTrue();
          continue;
        }
        assertThat(none).as(relation).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
        assertThat(none.rows).as(relation).isEmpty();

        assertThat(none.columns).as("the columns of %s with no row", relation).containsExactlyEntriesOf(all.columns);
        for (final Map.Entry<String, PostgresType> column : all.columns.entrySet())
          assertThat(column.getValue()).as("%s.%s", relation, column.getKey())
              .isEqualTo(PostgresCatalog.declaredType(relation, column.getKey()));
        checked.add(relation);
      }
      assertThat(checked).contains("pg_class", "pg_attribute", "pg_type", "information_schema.columns", "pg_index", "pg_constraint");
    }

    // Regression: #10869
    /**
     * The drift guard on the declarations: a value a relation carries is of the type the relation announces its column with, for every
     * row of every relation of a schema that populates all of them. A row builder that starts producing a different type, or a
     * column that starts carrying values without a declaration, fails here rather than in a client that typed its schema from the OIDs.
     */
    @Test
    void everyValueIsOfTheTypeItsColumnIsDeclaredWith() {
      int typedValues = 0;
      for (final String relation : new TreeSet<>(PostgresCatalog.modelledRelations())) {
        final PostgresCatalog.Answer all = PostgresCatalog.resolve("SELECT * FROM " + relation, database, USER);
        if (all == null || all == PostgresCatalog.DECLINED)
          continue;
        for (final Map<String, Object> row : all.rows)
          for (final Map.Entry<String, Object> value : row.entrySet()) {
            if (value.getValue() == null)
              continue;
            assertThat(typeOf(value.getValue())).as("%s.%s = %s", relation, value.getKey(), value.getValue())
                .isEqualTo(PostgresCatalog.declaredType(relation, value.getKey()));
            ++typedValues;
          }
      }
      assertThat(typedValues).isGreaterThan(500);
    }

    // Regression: #10869
    @Test
    void theShapesOfTheIssueAnnounceTheirColumnsWithNoRow() {
      assertSameColumns("SELECT oid, relnatts, relhasindex, relkind FROM pg_class WHERE relname = 'Probe'",
          "SELECT oid, relnatts, relhasindex, relkind FROM pg_class WHERE relname = 'NoSuchRelation'");
      assertSameColumns("SELECT attrelid, attname, attnum, atttypid, attnotnull FROM pg_attribute WHERE attname = 's'",
          "SELECT attrelid, attname, attnum, atttypid, attnotnull FROM pg_attribute WHERE attrelid = 0");
      assertSameColumns("SELECT table_name, ordinal_position, numeric_precision FROM information_schema.columns WHERE table_name='Probe'",
          "SELECT table_name, ordinal_position, numeric_precision FROM information_schema.columns WHERE table_name='NoSuch'");
      // aliases and qualifiers keep the column's type
      assertSameColumns("SELECT c.oid AS id, c.relhasindex FROM pg_class c WHERE c.relname = 'Probe'",
          "SELECT c.oid AS id, c.relhasindex FROM pg_class c WHERE c.relname = 'NoSuch'");

      final PostgresCatalog.Answer typed = PostgresCatalog.resolve(
          "SELECT oid, relnatts, relhasindex, relkind FROM pg_class WHERE relname = 'NoSuchRelation'", database, USER);
      assertThat(typed.columns).containsExactly(Map.entry("oid", PostgresType.INTEGER), Map.entry("relnatts", PostgresType.INTEGER),
          Map.entry("relhasindex", PostgresType.BOOLEAN), Map.entry("relkind", PostgresType.VARCHAR));
    }

    // Regression: #10869
    @Test
    void aSetOperationWithAnEmptyArmKeepsTheTypesOfItsColumns() {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(
          "SELECT oid, relhasindex FROM pg_class WHERE relname = 'NoSuch' UNION ALL SELECT oid, relhasindex FROM pg_class WHERE relname = 'Nope'",
          database, USER);
      assertThat(answer).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
      assertThat(answer.rows).isEmpty();
      assertThat(answer.columns).containsExactly(Map.entry("oid", PostgresType.INTEGER), Map.entry("relhasindex", PostgresType.BOOLEAN));
    }

    // Regression: #10869
    @Test
    void aTypeLookupThatFindsNoTypeStillAnnouncesTheProjectedColumns() {
      final PostgresCatalog.Answer byName = typeLookup("SELECT oid, typname, typlen FROM pg_type WHERE typname = 'int4'");
      final PostgresCatalog.Answer noName = typeLookup("SELECT oid, typname, typlen FROM pg_type WHERE typname = 'nosuchtype'");
      final PostgresCatalog.Answer byOid = typeLookup("SELECT oid, typname, typlen FROM pg_type WHERE oid = 23");
      final PostgresCatalog.Answer noOid = typeLookup("SELECT oid, typname, typlen FROM pg_type WHERE oid = 999999");

      assertThat(byName.rows).hasSize(1);
      assertThat(noName.rows).isEmpty();
      assertThat(noOid.rows).isEmpty();
      for (final PostgresCatalog.Answer answer : new PostgresCatalog.Answer[] { byName, noName, byOid, noOid })
        assertThat(answer.columns).containsExactly(Map.entry("oid", PostgresType.INTEGER), Map.entry("typname", PostgresType.VARCHAR),
            Map.entry("typlen", PostgresType.INTEGER));

      // the element self-join of a scalar selects nothing, under the columns it projected
      final PostgresCatalog.Answer scalarElement = typeLookup(
          "SELECT e.typdelim, e.typname FROM pg_type t, pg_type e WHERE t.oid = 23 AND t.typelem = e.oid");
      assertThat(scalarElement.rows).isEmpty();
      assertThat(scalarElement.columns.keySet()).containsExactly("typdelim", "typname");

      // the recogniser and the relational catalog announce pg_type's columns alike
      final PostgresCatalog.Answer relational = PostgresCatalog.resolve("SELECT * FROM pg_type", database, USER);
      assertThat(typeLookup("SELECT * FROM pg_type WHERE oid = 999999").columns).allSatisfy(
          (name, type) -> assertThat(type).as(name).isEqualTo(relational.columns.get(name)));
    }

    /** A lookup through the pg_type recogniser, with the row types of this test's database. */
    private PostgresCatalog.Answer typeLookup(final String query) {
      return PostgresTypeCatalog.resolveAnswer(query, () -> PostgresCatalog.rowTypes(database));
    }

    private void assertSameColumns(final String matching, final String empty) {
      final PostgresCatalog.Answer some = PostgresCatalog.resolve(matching, database, USER);
      final PostgresCatalog.Answer none = PostgresCatalog.resolve(empty, database, USER);
      assertThat(some.rows).as(matching).isNotEmpty();
      assertThat(none.rows).as(empty).isEmpty();
      assertThat(none.columns).as(empty).containsExactlyEntriesOf(some.columns);
    }

    /** The type a catalog value is announced as when it is sampled: what the declarations must agree with. */
    private static PostgresType typeOf(final Object value) {
      if (value instanceof PostgresCatalogIndexes.Vector)
        return PostgresType.VARCHAR;
      final PostgresType sampled = PostgresType.getTypeForValue(value);
      return sampled.isArrayType() || sampled.isNativeScalarType() ? sampled : PostgresType.VARCHAR;
    }
  }

  /**
   * Issue #10873: {@code pg_class.reltype} was 0 for every relation, so the join a client writes to reach a table's row type
   * matched nothing, and {@code pg_attribute.attcollation} was 0 for every column while {@code pg_type.typcollation} said the
   * column's type was collatable. Every relation now has its row type in {@code pg_type} ({@code typtype = 'c'}, {@code typrelid}
   * leading back), {@code reltype} names it, and the collation of a column is its type's.
   * <p>
   * The census generalises #10711's rule from the references of {@code pg_type} to every OID-valued column of every relation the
   * catalog models: each names a row of the relation it points into, and holds 0 only where PostgreSQL does. The columns are
   * enumerated from the catalog itself, and one that looks like a reference and is declared neither way fails the census, so a
   * relation or a column added later has to say what it points at.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RowType {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogColumnsTest-RowType";
    private static final String USER          = "root";

    /** The name shapes PostgreSQL gives a column that holds another catalog row's OID. */
    private static final Pattern LOOKS_LIKE_A_REFERENCE = Pattern.compile(
        ".+(oid|relid|typid|namespace|nspid|owner|collation|reltype|oftype|elem|array|relam|indid|parentid|dba|pubid|basetype|rewrite"
            + "|tablespace|classid|ev_class|foid|roleid|member|grantor|rettype|lang|support|variadic|server|subtype|subopc|canonical"
            + "|subdiff|constraint|aclrole)$");

    /** A column that holds a reference: the relation it points into, and when it may hold 0 instead. */
    private record Reference(String target, Predicate<Map<String, Object>> zeroAllowed) {
    }

    private static final Predicate<Map<String, Object>> NEVER  = row -> false;
    private static final Predicate<Map<String, Object>> ALWAYS = row -> true;

    private static final Map<String, Reference> REFERENCES = new HashMap<>();

    /** Columns whose name looks like a reference and that are not one, or point into nothing this catalog models, with why. */
    private static final Map<String, String> NOT_CHECKED = new HashMap<>();

    static {
      references("pg_class.relnamespace", "pg_namespace", NEVER);
      // the row type of the relation, 0 for an index alone (issue #10873)
      references("pg_class.reltype", "pg_type", row -> "i".equals(row.get("relkind")));
      references("pg_class.reloftype", "pg_type", ALWAYS);
      references("pg_class.relowner", "pg_roles", NEVER);
      references("pg_class.relam", "pg_am", ALWAYS);
      references("pg_class.reltoastrelid", "pg_class", ALWAYS);
      references("pg_class.relrewrite", "pg_class", ALWAYS);
      // 0 is the database's default tablespace in PostgreSQL
      references("pg_class.reltablespace", "pg_tablespace", ALWAYS);
      references("pg_attribute.attrelid", "pg_class", NEVER);
      references("pg_attribute.atttypid", "pg_type", NEVER);
      references("pg_attribute.attcollation", "pg_collation", ALWAYS);
      references("pg_type.typnamespace", "pg_namespace", NEVER);
      references("pg_type.typowner", "pg_roles", NEVER);
      references("pg_type.typrelid", "pg_class", ALWAYS);
      references("pg_type.typelem", "pg_type", ALWAYS);
      references("pg_type.typarray", "pg_type", ALWAYS);
      references("pg_type.typbasetype", "pg_type", ALWAYS);
      references("pg_type.typcollation", "pg_collation", ALWAYS);
      references("pg_namespace.nspowner", "pg_roles", NEVER);
      references("pg_database.datdba", "pg_roles", NEVER);
      references("pg_database.dattablespace", "pg_tablespace", ALWAYS);
      references("pg_index.indexrelid", "pg_class", NEVER);
      references("pg_index.indrelid", "pg_class", NEVER);
      references("pg_index.indcollation", "pg_collation", ALWAYS);
      references("pg_constraint.connamespace", "pg_namespace", NEVER);
      references("pg_constraint.conrelid", "pg_class", ALWAYS);
      references("pg_constraint.contypid", "pg_type", ALWAYS);
      references("pg_constraint.conindid", "pg_class", ALWAYS);
      references("pg_constraint.conparentid", "pg_constraint", ALWAYS);
      references("pg_constraint.confrelid", "pg_class", ALWAYS);
      references("pg_attrdef.adrelid", "pg_class", NEVER);
      references("pg_inherits.inhrelid", "pg_class", NEVER);
      references("pg_inherits.inhparent", "pg_class", NEVER);
      references("pg_collation.collnamespace", "pg_namespace", NEVER);
      references("pg_collation.collowner", "pg_roles", NEVER);
      references("pg_tablespace.spcowner", "pg_roles", NEVER);
      references("pg_policy.polrelid", "pg_class", NEVER);
      references("pg_statistic_ext.stxrelid", "pg_class", NEVER);
      references("pg_statistic_ext.stxnamespace", "pg_namespace", NEVER);
      references("pg_statistic_ext.stxowner", "pg_roles", NEVER);
      references("pg_publication.pubowner", "pg_roles", NEVER);
      references("pg_publication_rel.prpubid", "pg_publication", NEVER);
      references("pg_publication_rel.prrelid", "pg_class", NEVER);
      references("pg_publication_namespace.pnpubid", "pg_publication", NEVER);
      references("pg_publication_namespace.pnnspid", "pg_namespace", NEVER);
      references("pg_range.rngtypid", "pg_type", NEVER);
      references("pg_range.rngsubtype", "pg_type", NEVER);
      references("pg_range.rngmultitypid", "pg_type", NEVER);
      references("pg_range.rngcollation", "pg_collation", ALWAYS);
      references("pg_enum.enumtypid", "pg_type", NEVER);
      references("pg_proc.pronamespace", "pg_namespace", NEVER);
      references("pg_proc.proowner", "pg_roles", NEVER);
      references("pg_proc.prorettype", "pg_type", NEVER);
      references("pg_proc.provariadic", "pg_type", ALWAYS);
      references("pg_trigger.tgrelid", "pg_class", NEVER);
      references("pg_trigger.tgparentid", "pg_trigger", ALWAYS);
      references("pg_trigger.tgfoid", "pg_proc", NEVER);
      references("pg_trigger.tgconstrrelid", "pg_class", ALWAYS);
      references("pg_trigger.tgconstrindid", "pg_class", ALWAYS);
      references("pg_trigger.tgconstraint", "pg_constraint", ALWAYS);
      references("pg_auth_members.roleid", "pg_roles", NEVER);
      references("pg_auth_members.member", "pg_roles", NEVER);
      references("pg_auth_members.grantor", "pg_roles", NEVER);
      references("pg_rewrite.ev_class", "pg_class", NEVER);
      references("pg_sequence.seqrelid", "pg_class", NEVER);
      references("pg_sequence.seqtypid", "pg_type", NEVER);
      references("pg_default_acl.defaclrole", "pg_roles", NEVER);
      references("pg_default_acl.defaclnamespace", "pg_namespace", ALWAYS);
      references("pg_foreign_table.ftrelid", "pg_class", NEVER);
      references("pg_event_trigger.evtowner", "pg_roles", NEVER);
      references("pg_foreign_data_wrapper.fdwowner", "pg_roles", NEVER);
      references("pg_foreign_server.srvowner", "pg_roles", NEVER);
      references("pg_subscription.subowner", "pg_roles", NEVER);
      references("pg_largeobject_metadata.lomowner", "pg_roles", NEVER);

      notChecked("pg_proc.prolang", "pg_language is not modelled, and pg_proc has no row");
      notChecked("pg_proc.prosupport", "a regproc, read as a name");
      notChecked("pg_foreign_table.ftserver", "pg_foreign_server is not modelled, and pg_foreign_table has no row");
      notChecked("pg_event_trigger.evtfoid", "pg_proc has no row, and pg_event_trigger has none either");
      notChecked("pg_subscription.subrunasowner", "a boolean: whether the subscription applies changes as the table owner, not an OID");
      notChecked("pg_foreign_server.srvfdw", "pg_foreign_data_wrapper has no row, and pg_foreign_server has none either");
      notChecked("pg_range.rngsubopc", "pg_opclass is not modelled, and pg_range has no row");
      notChecked("pg_range.rngcanonical", "a regproc, read as a name");
      notChecked("pg_range.rngsubdiff", "a regproc, read as a name");
      notChecked("pg_index.indclass", "pg_opclass is not modelled: the operator classes have no equivalent here");
      notChecked("pg_description.classoid", "the catalog of the object described, and pg_description has no row");
      notChecked("pg_shdescription.classoid", "the catalog of the object described, and pg_shdescription has no row");
      notChecked("pg_depend.classid",
          "the OID of a system catalog (pg_class, pg_constraint, pg_type, pg_rewrite), which pg_class does not list as a row");
      notChecked("pg_depend.refclassid",
          "the OID of a system catalog (pg_class, pg_constraint, pg_namespace, pg_rewrite), which pg_class does not list as a row");
      notChecked("pg_shdescription.objoid", "an object of any shared catalog, and pg_shdescription has no row");
      notChecked("pg_tables.tableowner", "the owner's name, not its OID");
      notChecked("pg_matviews.matviewowner", "the owner's name, not its OID");
      notChecked("pg_views.viewowner", "the owner's name, not its OID");
    }

    private static void references(final String column, final String target, final Predicate<Map<String, Object>> zeroAllowed) {
      REFERENCES.put(column, new Reference(target, zeroAllowed));
    }

    private static void notChecked(final String column, final String why) {
      NOT_CHECKED.put(column, why);
    }

    private Database database;

    @BeforeEach
    void createDatabase() {
      final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
      if (factory.exists())
        factory.open().drop();

      database = factory.create();
      // a type with a super type, a text column, an integer one with a default, a list of text, a unique index on the text
      // column, and a materialized view
      database.transaction(() -> {
        final Schema schema = database.getSchema();
        final VertexType person = schema.createVertexType("Person");
        person.createProperty("name", Type.STRING);
        person.createProperty("age", Type.INTEGER).setDefaultValue("0");
        person.createProperty("tags", Type.LIST, "STRING");
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

    // Regression: #10873
    /** The report's repro: the join from a relation to its row type answers one row per table and materialized view. */
    @Test
    void theRowTypeOfEveryRelationIsARowOfPgType() {
      final List<Map<String, Object>> relations = resolve("SELECT c.oid, c.relname, c.relkind, c.reltype FROM pg_class c ORDER BY 2").rows;
      final Set<String> withRowType = new TreeSet<>();
      for (final Map<String, Object> relation : relations)
        if ("i".equals(relation.get("relkind")))
          assertThat(number(relation, "reltype")).as("an index has no row type: %s", relation.get("relname")).isZero();
        else {
          assertThat(number(relation, "reltype")).as("the row type of %s", relation.get("relname")).isNotZero();
          withRowType.add((String) relation.get("relname"));
        }
      assertThat(withRowType).containsExactly("Employee", "Person", "PersonView");

      final List<Map<String, Object>> joined = resolve(
          "SELECT c.oid, c.relname, t.typname, t.typtype, t.typrelid, t.typcategory FROM pg_class c JOIN pg_type t ON t.oid = c.reltype "
              + "ORDER BY 2").rows;
      assertThat(joined).hasSize(3);
      for (final Map<String, Object> row : joined) {
        assertThat(row.get("typname")).isEqualTo(row.get("relname"));
        assertThat(row.get("typtype")).isEqualTo("c");
        assertThat(row.get("typcategory")).isEqualTo("C");
        assertThat(number(row, "typrelid")).as("typrelid leads back to the relation").isEqualTo(number(row, "oid"));
      }

      // the other direction, from the row type to its relation
      assertThat(column(resolve("SELECT c.relname FROM pg_type t JOIN pg_class c ON c.oid = t.typrelid WHERE t.typname = 'Person'"),
          "relname")).containsExactly("Person");
    }

    // Regression: #10873
    /** pg_type lists the row types in both of the ways it is answered: by this catalog and by the recogniser that runs first. */
    @Test
    void pgTypeListsTheRowTypesInBothOfItsAnswers() {
      assertThat(column(resolve("SELECT typname FROM pg_type WHERE typtype = 'c' ORDER BY typname"), "typname"))
          .containsExactly("Employee", "Person", "PersonView");

      final int rowType = number(resolve("SELECT reltype FROM pg_class WHERE relname = 'Person'").rows.getFirst(), "reltype");
      final List<Map<String, Object>> recognised = PostgresTypeCatalog.resolve("SELECT oid, typname, typtype, typrelid FROM pg_type",
          () -> PostgresCatalog.rowTypes(database));
      assertThat(recognised).filteredOn(row -> "c".equals(row.get("typtype"))).extracting(row -> row.get("typname"))
          .containsExactly("Employee", "Person", "PersonView");
      assertThat(PostgresTypeCatalog.resolve("SELECT typname, typrelid FROM pg_type WHERE oid = " + rowType,
          () -> PostgresCatalog.rowTypes(database))).extracting(row -> row.get("typname")).containsExactly("Person");
      assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type WHERE typname = 'Person'", () -> PostgresCatalog.rowTypes(database)))
          .extracting(row -> row.get("oid")).containsExactly(rowType);
      // a name is compared as written
      assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type WHERE typname = 'person'", () -> PostgresCatalog.rowTypes(database)))
          .isEmpty();
      // a row type has no element type
      assertThat(PostgresTypeCatalog.resolve("SELECT e.typdelim, e.typname FROM pg_type t, pg_type e WHERE t.oid = " + rowType
          + " AND t.typelem = e.oid", () -> PostgresCatalog.rowTypes(database))).isEmpty();
    }

    // Regression: #10873
    /** {@code format_type} and {@code regtype} name a row type as PostgreSQL prints it: the relation's name, quoted as needed. */
    @Test
    void aRowTypeIsNamedAfterItsRelation() {
      assertThat(column(resolve("SELECT format_type(c.reltype, NULL) AS t FROM pg_class c WHERE c.relname = 'Person'"), "t"))
          .containsExactly("\"Person\"");
      assertThat(column(resolve("SELECT c.reltype::regtype AS t FROM pg_class c WHERE c.relname = 'Person'"), "t"))
          .containsExactly("\"Person\"");
      final Object rowType = resolve("SELECT reltype FROM pg_class WHERE relname = 'Person'").rows.getFirst().get("reltype");
      assertThat(column(resolve("SELECT oid FROM pg_type WHERE oid = 'Person'::regtype"), "oid")).containsExactly(rowType);
    }

    // Regression: #10873
    /** The {@code attcollation} half: a column's collation is its type's, and an index key's is its column's. */
    @Test
    void theCollationOfAColumnIsTheCollationOfItsType() {
      final List<Map<String, Object>> rows = resolve("SELECT a.attname, a.attcollation, t.typname, t.typcollation FROM pg_attribute a "
          + "JOIN pg_type t ON t.oid = a.atttypid WHERE a.attrelid IN (SELECT oid FROM pg_class WHERE relname = 'Person')").rows;
      assertThat(rows).extracting(row -> row.get("attname")).contains("name", "age", "tags");
      for (final Map<String, Object> row : rows)
        assertThat(row.get("attcollation")).as("the collation of %s (%s)", row.get("attname"), row.get("typname"))
            .isEqualTo(row.get("typcollation"));
      assertThat(rows).filteredOn(row -> "name".equals(row.get("attname"))).extracting(row -> row.get("attcollation")).containsExactly(100);
      assertThat(rows).filteredOn(row -> "age".equals(row.get("attname"))).extracting(row -> row.get("attcollation")).containsExactly(0);

      assertThat(column(resolve("SELECT i.indcollation FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'Person[name]'"),
          "indcollation")).extracting(Object::toString).containsExactly("100");
    }

    // Regression: #10873
    /**
     * The guarantee: every reference column of every relation the catalog models names a row of the relation it points into, or
     * holds 0 where PostgreSQL does. The columns come from the catalog, so a new one shaped like a reference must be declared.
     */
    @Test
    void everyReferenceOfEveryRelationIsARowOfItsTarget() {
      final Set<String> undeclared = new TreeSet<>();
      final Map<String, Set<Long>> oidsOf = new HashMap<>();
      final List<String> violations = new ArrayList<>();
      final Set<String> checked = new TreeSet<>();

      for (final String relation : new TreeSet<>(PostgresCatalog.modelledRelations())) {
        if (relation.startsWith("information_schema."))
          continue;
        final PostgresCatalog.Answer answer = PostgresCatalog.resolve("select * from " + relation, database, USER);
        if (answer == null || answer.rows == null)
          continue;

        for (final String columnName : answer.columns.keySet()) {
          final String column = relation + "." + columnName.toLowerCase(Locale.ENGLISH);
          final Reference reference = REFERENCES.get(column);
          if (reference == null) {
            if (LOOKS_LIKE_A_REFERENCE.matcher(columnName.toLowerCase(Locale.ENGLISH)).matches() && !NOT_CHECKED.containsKey(column))
              undeclared.add(column);
            continue;
          }

          final Set<Long> targets = oidsOf.computeIfAbsent(reference.target(), this::oidsOf);
          for (final Map<String, Object> row : answer.rows)
            for (final long oid : oids(row.get(columnName))) {
              if (oid == 0) {
                if (!reference.zeroAllowed().test(row))
                  violations.add(column + " is 0 in " + row);
              } else if (!targets.contains(oid))
                violations.add(column + " = " + oid + " names no row of " + reference.target());
              checked.add(column);
            }
        }
      }

      assertThat(undeclared).as("columns shaped like a reference that the census does not declare").isEmpty();
      assertThat(violations).isEmpty();
      // the census reached the columns of the report, and the ones #10711 covered
      assertThat(checked).contains("pg_class.reltype", "pg_class.relnamespace", "pg_class.relam", "pg_attribute.attcollation",
          "pg_attribute.atttypid", "pg_type.typrelid", "pg_type.typnamespace", "pg_type.typarray", "pg_index.indcollation");
    }

    // ---------------------------------------------------------------- helpers

    private Set<Long> oidsOf(final String relation) {
      final Set<Long> oids = new HashSet<>();
      for (final Map<String, Object> row : resolve("select oid from " + relation).rows)
        oids.addAll(oids(row.get("oid")));
      return oids;
    }

    /** The OIDs a value holds: none for a NULL, one for a number, each element of a vector, which a client reads as text. */
    private static List<Long> oids(final Object value) {
      final List<Long> oids = new ArrayList<>();
      if (value instanceof Number number)
        oids.add(number.longValue());
      else if (value != null && !value.toString().isBlank())
        for (final String element : value.toString().trim().split("\\s+"))
          oids.add(Long.parseLong(element));
      return oids;
    }

    private PostgresCatalog.Answer resolve(final String query) {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
      assertThat(answer).as("answered: %s", query).isNotNull();
      assertThat(answer.rows).as("answered rather than declined: %s", query).isNotNull();
      return answer;
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
   * Issue #10799: the backing type of a materialized view or of a continuous aggregate declares no property, so the catalog, which
   * built a relation's columns from the declared properties of its type, described the view with {@code relnatts = 0} and no
   * {@code pg_attribute} row: {@code \d "PersonView"} printed a relation with no column.
   * <p>
   * A view's columns are the ones its query projects, in the order it projects them, typed as a statement Describe of that query
   * types them; a property the backing type does declare - the bucket of a continuous aggregate - is typed from its declaration.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MaterializedViewColumns {
    private static final String DATABASE_PATH = "./target/databases/PostgresCatalogColumnsTest-MaterializedViewColumns";
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

    // Regression: #10799
    /** The report's repro: the view has the column its query projects, and relnatts counts it. */
    @Test
    void theViewHasTheColumnItsQueryProjects() {
      assertThat(column(resolve("select relnatts from pg_class where relname = 'PersonView'"), "relnatts")).containsExactly(1);
      assertThat(column(resolve("""
          select attname from pg_attribute a join pg_class c on c.oid = a.attrelid where c.relname = 'PersonView'"""), "attname"))
          .containsExactly("name");
    }

    // Regression: #10799
    /** In the order the query projects them, under their alias, typed from the property each reads. */
    @Test
    void theColumnsFollowTheProjection() {
      final PostgresCatalog.Answer answer = attributes("Projected");
      assertThat(column(answer, "attname")).containsExactly("age", "label");
      assertThat(column(answer, "attnum")).containsExactly(1, 2);
      assertThat(column(answer, "type")).containsExactly("integer", "character varying");
    }

    // Regression: #10799
    @Test
    void anAggregateIsTypedAsADescribeTypesIt() {
      final PostgresCatalog.Answer answer = attributes("Counted");
      assertThat(column(answer, "attname")).containsExactly("name", "total");
      assertThat(column(answer, "type")).containsExactly("character varying", "bigint");
    }

    // Regression: #10799
    /** A query with no projection stores the whole record of the source, so the view has the columns of the source. */
    @Test
    void aViewOfWholeRecordsHasTheColumnsOfItsSource() {
      final PostgresCatalog.Answer answer = attributes("Everything");
      assertThat(column(answer, "attname")).containsExactly("age", "name");
      assertThat(column(answer, "type")).containsExactly("integer", "character varying");
    }

    // Regression: #10799
    /** The bucket of a continuous aggregate is declared on its backing type, and its declaration is what types it. */
    @Test
    void theBucketOfAContinuousAggregateIsATimestamp() {
      final PostgresCatalog.Answer answer = attributes("Hourly");
      assertThat(column(answer, "attname")).containsExactly("h", "peak");
      assertThat(column(answer, "type")).containsExactly("timestamp without time zone", "double precision");
    }

    // Regression: #10799
    /** relnatts of every relation counts the pg_attribute rows of it, views included. */
    @Test
    void relnattsOfEveryViewCountsItsAttributeRows() {
      for (final Map<String, Object> view : resolve("select oid, relname, relnatts from pg_class where relkind = 'm'").rows) {
        final PostgresCatalog.Answer count = resolve("select count(*) from pg_attribute where attrelid = " + view.get("oid"));
        assertThat(((Number) count.rows.get(0).get("count")).intValue()).as("the columns of %s", view.get("relname"))
            .isEqualTo(view.get("relnatts")).isPositive();
      }
    }

    // Regression: #10799
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

  /**
   * Issue #10691: the catalog built its relations from {@code Schema.getTypes()} and classified every one of them as an ordinary
   * table, so the backing type of a materialized view - and of a continuous aggregate, which is created the same way - was listed
   * as a {@code BASE TABLE} with {@code relkind = 'r'}, while {@code pg_matviews} was refused and {@code \dm} answered nothing.
   * <p>
   * A materialized view is described here where PostgreSQL describes one: {@code pg_class} with {@code relkind = 'm'} and a row of
   * {@code pg_matviews}. PostgreSQL lists none in {@code pg_tables}, {@code pg_views}, {@code information_schema.tables} or
   * {@code information_schema.views}: the first two hold kinds 'r' and 'v', the last two the kinds 'r', 'v', 'f' and 'p'.
   */
  @Nested
  class MaterializedViewKind {
    private static final String DATABASE_PATH   = "./target/databases/PostgresCatalogColumnsTest-MaterializedViewKind";
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

    // Regression: #10691
    /** The report's own query: a materialized view and a continuous aggregate are relations of kind 'm', not tables. */
    @Test
    void pgClassClassifiesEachRelationByWhatItIs() {
      final PostgresCatalog.Answer answer = resolve("select relname, relkind from pg_class where relkind <> 'i' order by 1");

      assertThat(column(answer, "relname")).containsExactly("AggView", "Person", "PersonView", "Reading");
      assertThat(column(answer, "relkind")).containsExactly("m", "r", "m", "r");
    }

    // Regression: #10691
    @Test
    void theTableListsHoldOnlyTheTables() {
      assertThat(column(resolve("select tablename from pg_tables order by 1"), "tablename")).containsExactly("Person", "Reading");
      final PostgresCatalog.Answer tables = resolve("select table_name, table_type from information_schema.tables order by 1");
      assertThat(column(tables, "table_name")).containsExactly("Person", "Reading");
      assertThat(column(tables, "table_type")).containsOnly("BASE TABLE");
    }

    // Regression: #10691
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

    // Regression: #10691
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

    // Regression: #10691
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

    // Regression: #10691
    /** An ungrouped count over pg_matviews answers its one row, as it does over every relation this catalog models. */
    @Test
    void pgMatviewsIsModelledRatherThanRefused() {
      assertThat(column(resolve("select count(*) from pg_matviews"), "count")).containsExactly(2L);
      assertThat(column(resolve("select count(*) from pg_matviews where 1 = 0"), "count")).containsExactly(0L);
    }

    // Regression: #10691
    /** PostgreSQL keeps a materialized view out of both view listings: they hold relkind 'v', which ArcadeDB has none of. */
    @Test
    void theViewListingsHoldNoMaterializedView() {
      assertThat(resolve("select viewname from pg_views").rows).isEmpty();
      assertThat(resolve("select table_name from information_schema.views").rows).isEmpty();
    }

    // Regression: #10691
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

    // Regression: #10691
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

    // Regression: #10691
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
}
