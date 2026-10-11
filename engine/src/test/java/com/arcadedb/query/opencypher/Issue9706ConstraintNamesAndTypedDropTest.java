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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #9706: a constraint created without a name could not be dropped (it had no name and there was
 * no {@code SHOW CONSTRAINTS} to find one), and dropping an {@code IS TYPED} constraint left the type restriction in place.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9706ConstraintNamesAndTypedDropTest extends TestHelper {

  @Test
  void anUnnamedNotNullConstraintIsListedAndDroppable() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS NOT NULL");

    final Map<String, Object> row = onlyRow("SHOW CONSTRAINTS");
    assertThat(row.get("name")).isEqualTo("constraint_L_p_existence");
    assertThat(row.get("type")).isEqualTo("NODE_PROPERTY_EXISTENCE");
    assertThat(row.get("entityType")).isEqualTo("NODE");
    assertThat(row.get("labelsOrTypes")).isEqualTo(List.of("L"));
    assertThat(row.get("properties")).isEqualTo(List.of("p"));

    assertThat(dropped("DROP CONSTRAINT constraint_L_p_existence")).isEqualTo(1);
    // the flag is cleared, and the STRING declaration the constraint made for itself goes with it
    assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
    assertThat(rows("SHOW CONSTRAINTS")).isEmpty();
    database.transaction(() -> database.newVertex("L").save());
  }

  @Test
  void anUnnamedTypedConstraintIsListedAndDroppingItLiftsTheType() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: INTEGER");

    final Map<String, Object> row = onlyRow("SHOW CONSTRAINTS");
    assertThat(row.get("name")).isEqualTo("constraint_L_p_type");
    assertThat(row.get("type")).isEqualTo("NODE_PROPERTY_TYPE");
    assertThat(row.get("propertyType")).isEqualTo("INTEGER");

    command("DROP CONSTRAINT constraint_L_p_type");

    assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
    // a value of another type is accepted as written, not refused or converted
    command("CREATE (:L {p: 'text'})");
    assertThat(single("MATCH (n:L) RETURN n.p AS p").<String>getProperty("p")).isEqualTo("text");
  }

  @Test
  void aTypedConstraintThatReplacedADeclarationPutsItBack() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.p STRING");

    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.p IS :: INTEGER");
    assertThat(property("L", "p").getType()).isEqualTo(Type.LONG);

    command("DROP CONSTRAINT t");
    assertThat(property("L", "p").getType()).isEqualTo(Type.STRING);
  }

  @Test
  void aTypedConstraintThatCannotPutItsDeclarationBackStaysInPlace() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.p INTEGER");

    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.p IS :: STRING");
    command("CREATE (:L {p: 'not a number'})");

    assertThatThrownBy(() -> command("DROP CONSTRAINT t")).cause().isInstanceOf(SchemaException.class)
        .hasMessageContaining("cannot be restored").hasMessageContaining("INTEGER");
    // nothing was undone, and the constraint can still be found and dropped once the record is fixed
    assertThat(property("L", "p").getType()).isEqualTo(Type.STRING);
    assertThat(onlyRow("SHOW CONSTRAINTS").get("name")).isEqualTo("t");

    command("MATCH (n:L) SET n.p = '12'");
    command("DROP CONSTRAINT t");
    assertThat(property("L", "p").getType()).isEqualTo(Type.INTEGER);
  }

  @Test
  void theDeclarationStaysWhileAnotherConstraintStillNeedsIt() {
    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.p IS :: INTEGER");
    command("CREATE CONSTRAINT nn FOR (n:L) REQUIRE n.p IS NOT NULL");

    command("DROP CONSTRAINT t");
    // the NOT NULL needs a declared property for its MANDATORY flag: it owns the declaration now
    assertThat(property("L", "p").isMandatory()).isTrue();
    assertThat(names("SHOW CONSTRAINTS")).containsExactly("nn");

    command("DROP CONSTRAINT nn");
    assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
  }

  @Test
  void theDeclarationAUniqueConstraintIsBuiltOverStaysUntilItGoes() {
    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.p IS :: INTEGER");
    command("CREATE CONSTRAINT u FOR (n:L) REQUIRE n.p IS UNIQUE");

    command("DROP CONSTRAINT t");
    assertThat(property("L", "p").getType()).isEqualTo(Type.LONG);

    command("DROP CONSTRAINT u");
    assertThat(database.getSchema().getType("L").getAllIndexes(false)).isEmpty();
    assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
  }

  @Test
  void aNotNullDroppedBeforeTheTypedConstraintOnTheSamePropertyHandsItsDeclarationOver() {
    command("CREATE CONSTRAINT nn FOR (n:L) REQUIRE n.p IS NOT NULL");
    // the NOT NULL declared the property STRING; the type constraint replaces that declaration
    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.p IS :: INTEGER");

    command("DROP CONSTRAINT nn");
    assertThat(property("L", "p").getType()).isEqualTo(Type.LONG);
    assertThat(property("L", "p").isMandatory()).isFalse();

    // the declaration the type constraint replaced left with the NOT NULL, so there is nothing to put back
    command("DROP CONSTRAINT t");
    assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
  }

  @Test
  void aConflictingTypeConstraintIsRefused() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: INTEGER");

    assertThatThrownBy(() -> command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: STRING")).rootCause()
        .isInstanceOf(SchemaException.class).hasMessageContaining("constraint_L_p_type");
    assertThat(property("L", "p").getType()).isEqualTo(Type.LONG);
  }

  @Test
  void repeatingAnUnnamedConstraintListsItOnce() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS UNIQUE");
    command("CREATE CONSTRAINT IF NOT EXISTS FOR (n:L) REQUIRE n.p IS UNIQUE");
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.q IS :: STRING");
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.q IS :: STRING");

    assertThat(names("SHOW CONSTRAINTS")).containsExactly("constraint_L_p_unique", "constraint_L_q_type");
  }

  @Test
  void anUnnamedNodeKeyDroppedByItsIndexNameIsUndoneWhole() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS NODE KEY");
    final Map<String, Object> row = onlyRow("SHOW CONSTRAINTS");
    assertThat(row.get("name")).isEqualTo("constraint_L_p_key");
    assertThat(row.get("type")).isEqualTo("NODE_KEY");
    final String index = (String) row.get("ownedIndex");
    assertThat(index).isNotNull();

    command("DROP CONSTRAINT `" + index + "`");

    assertThat(database.getSchema().existsIndex(index)).isFalse();
    assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
    assertThat(rows("SHOW CONSTRAINTS")).isEmpty();
  }

  @Test
  void whatSqlDeclaredIsListedAndDroppable() {
    database.command("sql", "CREATE VERTEX TYPE S");
    database.command("sql", "CREATE PROPERTY S.code STRING (mandatory true)");
    database.command("sql", "CREATE PROPERTY S.n LONG");
    database.command("sql", "CREATE PROPERTY S.k LONG");
    database.command("sql", "CREATE INDEX ON S (k) UNIQUE");

    final List<Map<String, Object>> rows = rows("SHOW CONSTRAINTS YIELD name, type, propertyType ORDER BY name");
    assertThat(rows).extracting(r -> r.get("name"))
        .containsExactly("S[k]", "constraint_S_code_existence", "constraint_S_code_type", "constraint_S_k_type", "constraint_S_n_type");
    assertThat(rows.get(0).get("type")).isEqualTo("UNIQUENESS");
    assertThat(rows.get(3).get("propertyType")).isEqualTo("INTEGER");

    command("DROP CONSTRAINT constraint_S_code_existence");
    assertThat(property("S", "code").isMandatory()).isFalse();

    command("DROP CONSTRAINT constraint_S_n_type");
    assertThat(database.getSchema().getType("S").getPropertyIfExists("n")).isNull();

    // the declaration an index is built over cannot go while the index is there
    assertThatThrownBy(() -> command("DROP CONSTRAINT constraint_S_k_type")).rootCause().isInstanceOf(SchemaException.class)
        .hasMessageContaining("index");

    command("DROP CONSTRAINT `S[k]`");
    assertThat(database.getSchema().existsIndex("S[k]")).isFalse();
  }

  @Test
  void showConstraintsFilters() {
    command("CREATE CONSTRAINT u FOR (n:L) REQUIRE n.a IS UNIQUE");
    command("CREATE CONSTRAINT e FOR (n:L) REQUIRE n.b IS NOT NULL");
    command("CREATE CONSTRAINT k FOR (n:L) REQUIRE n.c IS NODE KEY");
    command("CREATE CONSTRAINT t FOR (n:L) REQUIRE n.d IS :: LIST<STRING NOT NULL>");
    command("CREATE CONSTRAINT r FOR ()-[r:R]-() REQUIRE r.x IS UNIQUE");

    assertThat(names("SHOW CONSTRAINTS")).containsExactly("e", "k", "r", "t", "u");
    assertThat(names("SHOW ALL CONSTRAINTS")).containsExactly("e", "k", "r", "t", "u");
    assertThat(names("SHOW UNIQUE CONSTRAINTS")).containsExactly("r", "u");
    assertThat(names("SHOW NODE UNIQUENESS CONSTRAINTS")).containsExactly("u");
    assertThat(names("SHOW RELATIONSHIP UNIQUE CONSTRAINTS")).containsExactly("r");
    assertThat(names("SHOW EXISTENCE CONSTRAINTS")).containsExactly("e");
    assertThat(names("SHOW KEY CONSTRAINTS")).containsExactly("k");
    assertThat(names("SHOW PROPERTY TYPE CONSTRAINTS")).containsExactly("t");
    assertThat(names("SHOW CONSTRAINTS WHERE name STARTS WITH 'u'")).containsExactly("u");

    final Map<String, Object> typed = onlyRow("SHOW CONSTRAINTS YIELD name, propertyType, createStatement WHERE name = 't'");
    assertThat(typed.get("propertyType")).isEqualTo("LIST<STRING NOT NULL>");
    assertThat(typed.get("createStatement")).isEqualTo("CREATE CONSTRAINT `t` FOR (n:`L`) REQUIRE n.`d` IS :: LIST<STRING NOT NULL>");

    assertThat(onlyRow("SHOW CONSTRAINTS WHERE name = 'r'").get("type")).isEqualTo("RELATIONSHIP_UNIQUENESS");
  }

  @Test
  void showConstraintsIsAReadOnlyQuery() {
    command("CREATE CONSTRAINT u FOR (n:L) REQUIRE n.a IS UNIQUE");
    try (final ResultSet rs = database.query("opencypher", "SHOW CONSTRAINTS")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("u");
    }
  }

  @Test
  void showIndexesNamesTheConstraintThatOwnsTheIndex() {
    command("CREATE CONSTRAINT u FOR (n:L) REQUIRE n.a IS UNIQUE");
    command("CREATE INDEX FOR (n:L) ON (n.b)");

    final List<Map<String, Object>> indexes = rows("SHOW INDEXES YIELD name, type, properties, owningConstraint, state ORDER BY name");
    assertThat(indexes).hasSize(2);
    assertThat(indexes.get(0).get("properties")).isEqualTo(List.of("a"));
    assertThat(indexes.get(0).get("owningConstraint")).isEqualTo("u");
    assertThat(indexes.get(0).get("state")).isEqualTo("ONLINE");
    assertThat(indexes.get(1).get("properties")).isEqualTo(List.of("b"));
    assertThat(indexes.get(1).get("owningConstraint")).isNull();
    assertThat(names("SHOW RANGE INDEXES")).hasSize(2);
    assertThat(names("SHOW FULLTEXT INDEXES")).isEmpty();
  }

  @Test
  void twoPropertySetsSpellingTheSameGeneratedNameGetTwoNames() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE (n.a_b, n.c) IS UNIQUE");
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE (n.a, n.b_c) IS UNIQUE");

    assertThat(names("SHOW CONSTRAINTS")).containsExactly("constraint_L_a_b_c_unique", "constraint_L_a_b_c_unique_2");
    command("DROP CONSTRAINT constraint_L_a_b_c_unique_2");
    assertThat(rows("SHOW CONSTRAINTS YIELD name, properties")).singleElement()
        .satisfies(r -> assertThat(r.get("properties")).isEqualTo(List.of("a_b", "c")));
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void concurrentUnnamedConstraintsAreRecordedOnceEachUnderDistinctNames() throws Exception {
    database.getSchema().createVertexType("L");
    final String[] statements = { "CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS NOT NULL",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:L) REQUIRE (n.a_b, n.c) IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (n:L) REQUIRE (n.a, n.b_c) IS UNIQUE" };
    final List<Thread> threads = new ArrayList<>();
    final List<Throwable> failures = new CopyOnWriteArrayList<>();
    for (int t = 0; t < 12; t++) {
      final String statement = statements[t % statements.length];
      final Thread thread = new Thread(() -> {
        try {
          command(statement);
        } catch (final Throwable e) {
          failures.add(e);
        }
      });
      threads.add(thread);
      thread.start();
    }
    for (final Thread thread : threads)
      thread.join();

    assertThat(failures).extracting(e -> e.getCause() != null ? e.getCause().toString() : e.toString()).isEmpty();
    assertThat(names("SHOW CONSTRAINTS")).containsExactly("constraint_L_a_b_c_unique", "constraint_L_a_b_c_unique_2",
        "constraint_L_p_existence");
  }

  @Test
  void twoLabelsSpellingTheSameGeneratedNameGetTwoNames() {
    command("CREATE CONSTRAINT FOR (n:A) REQUIRE n.b_c IS NOT NULL");
    command("CREATE CONSTRAINT FOR (n:A_b) REQUIRE n.c IS NOT NULL");
    // repeating either one changes nothing and records nothing new
    command("CREATE CONSTRAINT IF NOT EXISTS FOR (n:A_b) REQUIRE n.c IS NOT NULL");

    assertThat(names("SHOW CONSTRAINTS")).containsExactly("constraint_A_b_c_existence", "constraint_A_b_c_existence_2");

    command("DROP CONSTRAINT constraint_A_b_c_existence_2");
    assertThat(database.getSchema().getType("A_b").getPropertyIfExists("c")).isNull();
    assertThat(property("A", "b_c").isMandatory()).isTrue();
  }

  @Test
  void generatedNamesOfUnusualIdentifiersRoundTripThroughShowAndDrop() {
    command("CREATE CONSTRAINT FOR (n:`My Label`) REQUIRE n.`my prop` IS NOT NULL");
    command("CREATE CONSTRAINT FOR (n:`My Label`) REQUIRE n.`odd``name` IS :: STRING");

    final List<Object> names = names("SHOW CONSTRAINTS");
    assertThat(names).containsExactly("constraint_My Label_my prop_existence", "constraint_My Label_odd`name_type");

    for (final Object name : names)
      command("DROP CONSTRAINT `" + ((String) name).replace("`", "``") + "`");
    assertThat(rows("SHOW CONSTRAINTS")).isEmpty();
    assertThat(database.getSchema().getType("My Label").getPropertyIfExists("my prop")).isNull();
  }

  @Test
  void generatedNamesSurviveReopen() {
    command("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: STRING");

    reopenDatabase();

    assertThat(names("SHOW CONSTRAINTS")).containsExactly("constraint_L_p_type");
    command("DROP CONSTRAINT constraint_L_p_type");
    assertThat(database.getSchema().getType("L").getPropertyIfExists("p")).isNull();
  }

  private void command(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private long dropped(final String cypher) {
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      return rs.getStatistics().orElseThrow().getConstraintsRemoved();
    }
  }

  private List<Map<String, Object>> rows(final String cypher) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next().toMap());
    }
    return rows;
  }

  private List<Object> names(final String cypher) {
    return rows(cypher).stream().map(r -> r.get("name")).toList();
  }

  private Map<String, Object> onlyRow(final String cypher) {
    final List<Map<String, Object>> rows = rows(cypher);
    assertThat(rows).hasSize(1);
    return rows.getFirst();
  }

  private Result single(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return rs.next();
    }
  }

  private Property property(final String typeName, final String propertyName) {
    final Property p = database.getSchema().getType(typeName).getPropertyIfExists(propertyName);
    assertThat(p).isNotNull();
    return p;
  }
}
