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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #13466: {@code SET n.p = EXISTS((n)-[:R]->(:L))} was refused as a pattern expression in a
 * SET value. A pattern inside a function-style {@code exists()} is a boolean check, as in RETURN (issue #4993); a bare pattern
 * is still refused.
 */
class CypherSetExistsPatternIssue13466Test extends TestHelper {
  private void createGraph() {
    database.transaction(() -> database.command("opencypher", "CREATE (:U {id: 1})-[:R]->(:L {id: 9}), (:U {id: 2})").close());
  }

  private List<Object> flagsById() {
    final List<Object> flags = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (u:U) RETURN u.has AS has ORDER BY u.id")) {
      while (rs.hasNext())
        flags.add(rs.next().getProperty("has"));
    }
    return flags;
  }

  @Test
  void setStoresTheBooleanOfAnExistsPattern() {
    createGraph();

    database.transaction(() -> database.command("opencypher", "MATCH (u:U) SET u.has = EXISTS((u)-[:R]->(:L))").close());

    assertThat(flagsById()).containsExactly(true, false);
  }

  @Test
  void setStoresTheNegationOfAnExistsPattern() {
    createGraph();

    database.transaction(() -> database.command("opencypher", "MATCH (u:U) SET u.has = NOT exists((u)-[:R]->(:L))").close());

    assertThat(flagsById()).containsExactly(false, true);
  }

  @Test
  void aBarePatternInASetValueIsStillRefused() {
    createGraph();

    assertThatThrownBy(() -> database.command("opencypher", "MATCH (u:U) SET u.has = (u)-[:R]->(:L)").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Pattern expressions are not allowed in SET values");
  }

  @Test
  void aBarePatternBesideAnExistsPatternIsStillRefused() {
    createGraph();

    assertThatThrownBy(
        () -> database.command("opencypher", "MATCH (u:U) SET u.has = [EXISTS((u)-[:R]->(:L)), (u)-[:R]->(:L)]").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Pattern expressions are not allowed in SET values");
  }
}
