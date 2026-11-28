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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #12916: a compound assignment on a nested target (a map key, a list element, a list inside a map) combined nothing and
 * stored the bare right-hand value. {@code t op= v} must leave the record exactly as {@code t = t op v} does.
 */
class Issue12916CompoundAssignmentNestedTest extends TestHelper {
  private static final String[] OPERATORS = { "+", "-", "*", "/" };
  // the grammar takes one modifier on the left of SET (m.c.d and m.b[1] are syntax errors), so these are all the shapes
  private static final String[] TARGETS   = { "x", "l", "m.a", "m['a']", "l[0]", "m.b" };
  private static final String[] VALUES    = { "3", "[5]", "'s'", "2.5" };

  @BeforeEach
  void createType() {
    database.transaction(() -> database.command("sql", "CREATE DOCUMENT TYPE C"));
  }

  @Test
  void issueRows() {
    assertThat(run("l[0] += 1").get("l")).isEqualTo(List.of(11, 20));
    assertThat(run("l[0] *= 3").get("l")).isEqualTo(List.of(30, 20));
    assertThat(((Map<?, ?>) run("m.a += 1").get("m")).get("a")).isEqualTo(2);
    assertThat(((Map<?, ?>) run("m.a -= 3").get("m")).get("a")).isEqualTo(-2);
    assertThat(((Map<?, ?>) run("m['a'] += 1").get("m")).get("a")).isEqualTo(2);
    assertThat(((Map<?, ?>) run("m.b += 3").get("m")).get("b")).isEqualTo(List.of(1, 2, 3));
    assertThat(run("x += 1").get("x")).isEqualTo(11);
    assertThat(run("l += 3").get("l")).isEqualTo(List.of(10, 20, 3));
  }

  @Test
  void nestedTargetFollowsTheTopLevelCounterRules() {
    // a NULL increment leaves the nested target as it is, as it does a top-level one
    assertThat(((Map<?, ?>) run("m.a += null").get("m")).get("a")).isEqualTo(1);
    assertThat(run("l[0] -= null").get("l")).isEqualTo(List.of(10, 20));
    assertThat(run("x += null").get("x")).isEqualTo(10);
    // a missing key starts from nothing: += sets the operand, -= its negative
    assertThat(((Map<?, ?>) run("m.z += 4").get("m")).get("z")).isEqualTo(4);
    assertThat(((Map<?, ?>) run("m.z -= 4").get("m")).get("z")).isEqualTo(-4);
  }

  @Test
  void compoundEqualsLongFormOnEveryTarget() {
    final List<String> mismatches = new ArrayList<>();
    int compared = 0;
    for (final String op : OPERATORS)
      for (final String target : TARGETS)
        for (final String value : VALUES) {
          final Object compound = outcome(target + " " + op + "= " + value);
          final Object longForm = outcome(target + " = " + target + " " + op + " " + value);
          ++compared;
          // by value, not by string form, so an Integer 2 against a Long 2 is a mismatch too
          if (!Objects.equals(compound, longForm))
            mismatches.add(target + " " + op + "= " + value + ": " + compound + " vs " + longForm);
        }
    assertThat(compared).isEqualTo(OPERATORS.length * TARGETS.length * VALUES.length);
    assertThat(mismatches).isEmpty();
  }

  private Object outcome(final String set) {
    try {
      return run(set);
    } catch (final Exception e) {
      return e.getClass().getSimpleName();
    }
  }

  private Map<String, Object> run(final String set) {
    final AtomicReference<Map<String, Object>> state = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "DELETE FROM C");
      database.command("sql", "INSERT INTO C CONTENT {\"x\": 10, \"l\": [10, 20], \"m\": {\"a\": 1, \"b\": [1, 2], \"c\": {\"d\": 7}}}");
      database.command("sql", "UPDATE C SET " + set);
      final Result r = database.query("sql", "SELECT x, l, m FROM C").next();
      state.set(r.toMap());
    });
    return state.get();
  }
}
