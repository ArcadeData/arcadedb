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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11598: in SQL MATCH the anonymous arrow {@code --} is the hop in both directions, as the grammar and
 * {@code .both()} say, but it was executed as {@code .out()}: the AST builder read the MINUS token list of the rule, which is
 * never null, as the sign of a {@code -->} arrow.
 */
class Issue11598MatchAnonymousBothArrowTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", "CREATE (x:A {id: 1})-[:R]->(:A {id: 2}), (:A {id: 3})-[:S]->(x)"));
  }

  @Test
  void anonymousArrowsFollowTheirDirection() {
    assertThat(ids("MATCH {type: A, as: a, where: (id = 1)}--{as: b} RETURN b")).containsExactly(2, 3);
    assertThat(ids("MATCH {type: A, as: a, where: (id = 1)}.both(){as: b} RETURN b")).containsExactly(2, 3);
    assertThat(ids("MATCH {type: A, as: a, where: (id = 1)}-->{as: b} RETURN b")).containsExactly(2);
    assertThat(ids("MATCH {type: A, as: a, where: (id = 1)}<--{as: b} RETURN b")).containsExactly(3);
    assertThat(ids("MATCH {type: A, as: a, where: (id = 1)}-S-{as: b} RETURN b")).containsExactly(3);
    assertThat(ids("MATCH {type: A, as: a, where: (id = 1)}-R->{as: b} RETURN b")).containsExactly(2);
    assertThat(ids("MATCH {type: A, as: a, where: (id = 1)}<-S-{as: b} RETURN b")).containsExactly(3);
  }

  @Test
  void anonymousBothArrowWithWhile() {
    assertThat(ids("MATCH {type: A, as: a, where: (id = 1)}--{as: b, while: ($depth < 1)} RETURN b")).containsExactly(1, 2, 3);
  }

  /** The same list test sent the {@code methodName{...}} form of a nested path down the arrow branch, as an edge type. */
  @Test
  void nestedPathMethodNameWithoutParentheses() {
    assertThat(rows("MATCH {type: A, as: a, where: (id = 1)}.(out{where: (id = 2)}) RETURN a")).isEqualTo(1);
    assertThat(rows("MATCH {type: A, as: a, where: (id = 1)}.(in{where: (id = 3)}) RETURN a")).isEqualTo(1);
    assertThat(rows("MATCH {type: A, as: a, where: (id = 1)}.(both{where: (id = 2 OR id = 3)}) RETURN a")).isEqualTo(2);
    assertThat(rows("MATCH {type: A, as: a, where: (id = 1)}.(--{where: (id = 2 OR id = 3)}) RETURN a")).isEqualTo(2);
    assertThat(rows("MATCH {type: A, as: a, where: (id = 1)}.(-->{where: (id = 2 OR id = 3)}) RETURN a")).isEqualTo(1);
  }

  private long rows(final String match) {
    try (final ResultSet rs = database.query("sql", match)) {
      return rs.stream().count();
    }
  }

  private List<Integer> ids(final String match) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT b.id AS i FROM (" + match + ") ORDER BY i")) {
      while (rs.hasNext())
        out.add(rs.next().<Number>getProperty("i").intValue());
    }
    return out;
  }
}
