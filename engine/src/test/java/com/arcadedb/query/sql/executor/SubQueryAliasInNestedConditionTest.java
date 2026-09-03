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
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins a sub-query used inside a condition that is evaluated against something other than the row being filtered -
 * {@code $nested CONTAINS (<condition>)}, where the condition sees each ITEM of {@code $nested} - which used to answer
 * nothing while the same condition written with a literal answered correctly (issue #7054, second report).
 * <p>
 * The planner lifts every sub-query into a LET and leaves a generated {@code _$$$SUBQUERY$$$_N} alias behind, so the
 * inner condition reads that alias from the command context. {@code CONTAINS} hands each item over as an
 * {@code Identifiable} when the collection holds records, and the identifier resolution for that shape only knew about
 * user-visible {@code $variable} names: the generated alias resolved to null, the {@code IN} then had nothing to test
 * against, and the whole condition was silently never true.
 * <p>
 * Every case therefore checks the sub-query spelling against the equivalent spelling that never went through the alias
 * (a literal, or an explicit LET variable), so the two cannot drift apart again.
 * <p>
 * The last case covers the third overload, which walks a MAP value: it had the opposite half of the same defect, asking
 * the context about every name instead of only the ones the context can own, so a plain map key was shadowed by any
 * context variable that happened to share it. All three now decide "context or record?" the same way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SubQueryAliasInNestedConditionTest extends TestHelper {

  private static final String SELECT_NESTED =
      "SELECT $nested.name AS names FROM AllSuppliers LET $nested = outE('RelSupplier').inV('Supplier') WHERE code = 'A1' ";

  private String s1Rid;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Supplier").createProperty("name", Type.STRING);
    database.getSchema().createVertexType("TopSuppliers").createProperty("name", Type.STRING);
    database.getSchema().getType("TopSuppliers").createProperty("target_rid", Type.LINK);
    database.getSchema().createVertexType("AllSuppliers").createProperty("code", Type.STRING);
    database.getSchema().createEdgeType("RelSupplier");
    database.getSchema().createDocumentType("Holder").createProperty("attrs", Type.MAP);

    database.transaction(() -> {
      for (final String name : new String[] { "S1", "S2", "S3" }) {
        database.command("sql", "INSERT INTO Supplier SET name = '" + name + "'");
        database.command("sql", "INSERT INTO TopSuppliers SET name = '" + name + "', target_rid = (SELECT @rid FROM Supplier "
            + "WHERE name = '" + name + "')");
      }
      database.command("sql", "INSERT INTO AllSuppliers SET code = 'A1'");
      // A1 is linked to S1 and S2, so a condition that fails to discriminate is visible as an over-match, not only as
      // the empty answer the report was about.
      database.command("sql", "CREATE EDGE RelSupplier FROM (SELECT FROM AllSuppliers WHERE code = 'A1') TO "
          + "(SELECT FROM Supplier WHERE name IN ['S1', 'S2'])");
      // A map whose key collides with a plausible LET variable name.
      database.newDocument("Holder").set("attrs", Map.of("name", "FROM_MAP")).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM Supplier WHERE name = 'S1'")) {
      s1Rid = rs.next().getProperty("rid").toString();
    }
  }

  @Test
  void ridInSubQueryInsideContainsMatchesTheLinkedRecord() {
    assertThat(nestedNames(SELECT_NESTED + "AND $nested CONTAINS (@rid IN (SELECT target_rid FROM TopSuppliers "
        + "WHERE name = 'S1'))")).containsExactly("S1", "S2");
  }

  @Test
  void ridInSubQueryInsideContainsAnswersLikeTheLiteralSpelling() {
    assertThat(nestedNames(SELECT_NESTED + "AND $nested CONTAINS (@rid IN (SELECT target_rid FROM TopSuppliers "
        + "WHERE name = 'S1'))"))
        .isEqualTo(nestedNames(SELECT_NESTED + "AND $nested CONTAINS (@rid IN [" + s1Rid + "])"));
  }

  @Test
  void ridInSubQueryInsideContainsAnswersLikeAnExplicitLetVariable() {
    assertThat(nestedNames(SELECT_NESTED + "AND $nested CONTAINS (@rid IN (SELECT target_rid FROM TopSuppliers "
        + "WHERE name = 'S1'))"))
        .isEqualTo(nestedNames("SELECT $nested.name AS names FROM AllSuppliers LET $top = (SELECT target_rid FROM "
            + "TopSuppliers WHERE name = 'S1'), $nested = outE('RelSupplier').inV('Supplier') WHERE code = 'A1' "
            + "AND $nested CONTAINS (@rid IN $top)"));
  }

  @Test
  void aSubQueryInsideContainsIsNotJustAboutRids() {
    // The gap was in identifier resolution, not in the RID-address optimization, so a plain property comparison against
    // a lifted sub-query has to work too.
    assertThat(nestedNames(SELECT_NESTED + "AND $nested CONTAINS (name IN (SELECT name FROM TopSuppliers "
        + "WHERE name = 'S1'))")).containsExactly("S1", "S2");
  }

  @Test
  void aSubQueryInsideContainsThatMatchesNothingStillExcludesTheRow() {
    assertThat(nestedNames(SELECT_NESTED + "AND $nested CONTAINS (@rid IN (SELECT target_rid FROM TopSuppliers "
        + "WHERE name = 'nobody'))")).isEmpty();
  }

  @Test
  void aSubQueryInsideContainsThatMatchesNoLinkedItemExcludesTheRow() {
    // S3 exists, but A1 is not linked to it: the condition has to be false because no ITEM matches, which is a
    // different thing from the sub-query being empty.
    assertThat(nestedNames(SELECT_NESTED + "AND $nested CONTAINS (@rid IN (SELECT target_rid FROM TopSuppliers "
        + "WHERE name = 'S3'))")).isEmpty();
  }

  @Test
  void aNegatedSubQueryInsideContainsIsTheComplement() {
    // At least one linked supplier is NOT S1 - S2 is - so the row stays.
    assertThat(nestedNames(SELECT_NESTED + "AND $nested CONTAINS (@rid NOT IN (SELECT target_rid FROM TopSuppliers "
        + "WHERE name = 'S1'))")).containsExactly("S1", "S2");
    // ...while no linked supplier is outside {S1, S2}.
    assertThat(nestedNames(SELECT_NESTED + "AND $nested CONTAINS (@rid NOT IN (SELECT target_rid FROM TopSuppliers "
        + "WHERE name IN ['S1', 'S2']))")).isEmpty();
  }

  @Test
  void aMapKeyIsNotShadowedByAContextVariableOfTheSameName() {
    // The third place that had to decide "context variable or property of the row?" is the overload that walks a MAP
    // value, and it used to ask the context about every name rather than only the ones the context can own. A `LET
    // $name` therefore hijacked the map's own `name` key.
    assertThat(single("SELECT $m.name AS v FROM Holder LET $m = attrs")).isEqualTo("FROM_MAP");
    assertThat(single("SELECT $m.name AS v FROM Holder LET $name = 'SHADOW', $m = attrs")).isEqualTo("FROM_MAP");
    assertThat(single("SELECT $m.name AS v FROM Holder LET $name = 'SHADOW', $m = attrs WHERE $m.name = 'FROM_MAP'"))
        .isEqualTo("FROM_MAP");
  }

  /** The single value the query projects under {@code v}, or null when it returned no row. */
  private Object single(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.hasNext() ? rs.next().getProperty("v") : null;
    }
  }

  /** The names the single AllSuppliers row projects, or an empty list when the WHERE clause excluded it. */
  private List<String> nestedNames(final String query) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Object value = rs.next().getProperty("names");
        if (value instanceof Iterable<?> iterable)
          for (final Object name : iterable)
            names.add((String) name);
        else if (value != null)
          names.add((String) value);
      }
    }
    names.sort(null);
    return names;
  }
}
