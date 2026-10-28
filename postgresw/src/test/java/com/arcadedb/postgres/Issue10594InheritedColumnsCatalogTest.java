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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10594: {@code pg_class.relnatts} counted the properties a type declares itself, hidden ones included, while
 * {@code pg_attribute} and {@code information_schema.columns} list its visible properties, inherited ones included; and every
 * column was reported as locally declared ({@code attislocal = true, attinhcount = 0}) although {@code pg_inherits} names the
 * type's parents. PostgreSQL keeps {@code relnatts} equal to the number of {@code pg_attribute} rows of a relation, and marks a
 * column a relation did not declare itself with {@code attislocal = false} and {@code attinhcount} = the number of its direct
 * parents that carry it.
 */
class Issue10594InheritedColumnsCatalogTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10594InheritedColumnsCatalogTest";
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

  /** The issue's first query: a subtype's relnatts counts its inherited columns. */
  @Test
  void relnattsOfASubtypeCountsItsInheritedColumns() {
    final Map<String, Integer> relnatts = relnattsByTable();
    assertThat(relnatts.get("Super")).isEqualTo(2);
    assertThat(relnatts.get("Sub")).isEqualTo(3);
    assertThat(relnatts.get("SubSub")).isEqualTo(4);
    assertThat(relnatts.get("Bottom")).isEqualTo(3);
  }

  /** The correction on the issue: a hidden property is no column, so relnatts does not count it either. */
  @Test
  void relnattsDoesNotCountHiddenProperties() {
    final Map<String, Integer> relnatts = relnattsByTable();
    assertThat(relnatts.get("Flat")).isEqualTo(1);
    assertThat(relnatts.get("HiddenParent")).isEqualTo(1);
    assertThat(relnatts.get("HiddenChild")).isEqualTo(2);
  }

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

  /** A name both declared by the type and inherited is one column, local and inherited once, as PostgreSQL merges it. */
  @Test
  void aNameDeclaredAndInheritedIsOneMergedColumn() {
    assertThat(attributes("Merged")).containsExactly("m:true:1");
    assertThat(relnattsByTable().get("Merged")).isEqualTo(1);
  }

  /** A parent whose declaration of the name is hidden lists no such column, so the child's column inherits from no parent. */
  @Test
  void aHiddenParentDeclarationIsNotCountedAsInherited() {
    assertThat(attributes("Shadowing")).containsExactly("m:true:0");
    assertThat(attributes("HiddenMergedParent")).isEmpty();
  }

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
