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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9247: a statement of a sqlscript was cached under its printed text, which prints every positional parameter as
 * {@code ?} whatever its number, so statements that read the same text from different parameter positions shared one plan.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9247ScriptPositionalPlanCacheTest extends TestHelper {

  private void setup(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".sku STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".brand STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".n INTEGER");
    database.transaction(() -> {
      database.newDocument(type).set("sku", "S1").set("brand", "b1").set("n", 1).save();
      database.newDocument(type).set("sku", "S2").set("brand", "b2").set("n", 2).save();
      database.newDocument(type).set("sku", "S3").set("brand", "b3").set("n", 3).save();
    });
  }

  private void setupRef(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.newDocument(type).set("sku", "S" + i).set("ref", "r" + i).save();
    });
  }

  private static String render(final ResultSet rs) {
    final List<String> out = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      out.add(r.getProperty("sku") + ":" + r.getProperty("brand"));
    }
    return out.isEmpty() ? "(none)" : String.join(" ", out);
  }

  private String script(final String script, final Object... params) {
    try (final ResultSet rs = database.command("sqlscript", script, params)) {
      return render(rs);
    }
  }

  private String scriptNamed(final String script, final Map<String, Object> params) {
    try (final ResultSet rs = database.command("sqlscript", script, params)) {
      return render(rs);
    }
  }

  private String query(final String sql, final Object... params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return render(rs);
    }
  }

  private String queryNamed(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      return render(rs);
    }
  }

  @Test
  void twoSelectsInOneScript() {
    setup("A");
    assertThat(script("SELECT FROM A WHERE sku = ?; SELECT FROM A WHERE sku = ?;", "S1", "S2")).isEqualTo("S2:b2");
  }

  @Test
  void selectAloneThenScript() {
    setup("B");
    query("SELECT FROM B WHERE sku = ?", "S1");
    assertThat(script("SELECT FROM B WHERE brand = ?; SELECT FROM B WHERE sku = ?;", "b3", "S2")).isEqualTo("S2:b2");
  }

  @Test
  void scriptThenSelectAlone() {
    setup("C");
    script("SELECT FROM C WHERE brand = ?; SELECT FROM C WHERE sku = ?;", "b3", "S1");
    assertThat(query("SELECT FROM C WHERE sku = ?", "S2")).isEqualTo("S2:b2");
  }

  @Test
  void withUniqueIndex() {
    setup("D");
    database.command("sql", "CREATE INDEX ON D (sku) UNIQUE");
    assertThat(script("SELECT FROM D WHERE sku = ?; SELECT FROM D WHERE sku = ?;", "S1", "S2")).isEqualTo("S2:b2");
  }

  @Test
  void lowerCaseSecondSelect() {
    setup("E");
    assertThat(script("SELECT FROM E WHERE sku = ?; select from E where sku = ?;", "S1", "S2")).isEqualTo("S2:b2");
  }

  @Test
  void namedParametersAndLiterals() {
    setup("F");
    assertThat(scriptNamed("SELECT FROM F WHERE sku = :a; SELECT FROM F WHERE sku = :b;", Map.of("a", "S1", "b", "S2"))).isEqualTo("S2:b2");
    setup("G");
    assertThat(script("SELECT FROM G WHERE sku = 'S1'; SELECT FROM G WHERE sku = 'S2';")).isEqualTo("S2:b2");
  }

  @Test
  void sameScriptRepeatedStaysRight() {
    setup("K");
    for (int i = 0; i < 3; i++)
      assertThat(script("SELECT FROM K WHERE sku = ?; SELECT FROM K WHERE sku = ?;", "S1", "S2")).isEqualTo("S2:b2");
  }

  @Test
  void inSubqueryNamedParameterBoundByPositionAlone() {
    setup("H");
    setupRef("HR");
    assertThat(query("SELECT FROM H WHERE sku IN (SELECT sku FROM HR WHERE ref = :r)", "r2")).isEqualTo("S2:b2");
  }

  @Test
  void inSubqueryNamedParameterBoundByPosition() {
    setup("I");
    setupRef("IR");
    query("SELECT FROM I WHERE n > :lo AND sku IN (SELECT sku FROM IR WHERE ref = :r)", 0, "r1");
    assertThat(query("SELECT FROM I WHERE sku IN (SELECT sku FROM IR WHERE ref = :r)", "r2")).isEqualTo("S2:b2");
  }

  @Test
  void inSubqueryNamedParameterBoundByName() {
    setup("J");
    setupRef("JR");
    queryNamed("SELECT FROM J WHERE n > :lo AND sku IN (SELECT sku FROM JR WHERE ref = :r)", Map.of("lo", 0, "r", "r1"));
    assertThat(queryNamed("SELECT FROM J WHERE sku IN (SELECT sku FROM JR WHERE ref = :r)", Map.of("r", "r2"))).isEqualTo("S2:b2");
  }

  @Test
  void dmlInScriptWithOffsetParameter() {
    setup("L");
    for (int i = 0; i < 2; i++) {
      database.transaction(() -> database.command("sqlscript",
          "UPDATE L SET brand = 'x' WHERE sku = ?; UPDATE L SET brand = 'y' WHERE sku = ?;", "S1", "S2").close());
      assertThat(query("SELECT FROM L ORDER BY sku")).isEqualTo("S1:x S2:y S3:b3");
      database.transaction(() -> database.command("sql", "UPDATE L SET brand = 'b'").close());
    }
    database.transaction(() -> database.command("sqlscript", "DELETE FROM L WHERE sku = ?; DELETE FROM L WHERE sku = ?;", "S1", "S2").close());
    assertThat(query("SELECT FROM L")).isEqualTo("S3:b");
  }

  @Test
  void uncorrelatedAndParentInSubqueryUnchanged() {
    setup("M");
    setupRef("MR");
    assertThat(query("SELECT FROM M WHERE sku IN (SELECT sku FROM MR WHERE ref = 'r2')")).isEqualTo("S2:b2");
    assertThat(query("SELECT FROM M WHERE sku IN (SELECT sku FROM MR WHERE ref = 'r' + $parent.$current.n)")).isEqualTo("S1:b1 S2:b2 S3:b3");
  }
}
