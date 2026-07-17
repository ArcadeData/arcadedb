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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5315: a {@code rid:} filter inside a SQL MATCH pattern was silently
 * discarded by the parser, so the MATCH planner built an internal SELECT with no target and threw
 * {@code UnsupportedOperationException}. The RID should bind the alias to that single record and be
 * used as the traversal root, both as a literal and as a bound parameter.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MatchRidFilterTest extends TestHelper {
  public MatchRidFilterTest() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Friend");
    database.command("sql", "CREATE VERTEX Person SET name = 'n1'");
    database.command("sql", "CREATE VERTEX Person SET name = 'n2'");
    database.command("sql", "CREATE VERTEX Person SET name = 'n3'");
    database.command("sql",
        "CREATE EDGE Friend FROM (SELECT FROM Person WHERE name = 'n1') TO (SELECT FROM Person WHERE name = 'n2')");
    database.command("sql",
        "CREATE EDGE Friend FROM (SELECT FROM Person WHERE name = 'n1') TO (SELECT FROM Person WHERE name = 'n3')");
    database.commit();
    database.begin();
  }

  private RID ridOf(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE name = ?", name)) {
      return rs.next().getIdentity().get();
    }
  }

  @Test
  void literalRidAsRoot() {
    final RID rid = ridOf("n1");

    try (final ResultSet rs = database.query("sql",
        "SELECT expand(u) FROM (MATCH {rid: " + rid + ", as: u} RETURN u)")) {
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(item.<String>getProperty("name")).isEqualTo("n1");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void literalRidAsTraversalRoot() {
    final RID rid = ridOf("n1");

    try (final ResultSet rs = database.query("sql",
        "MATCH {rid: " + rid + ", as: u}-Friend->{as: x} RETURN x.name AS name ORDER BY name")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("n2");
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("n3");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void parameterizedRidAsRoot() {
    final RID rid = ridOf("n1");

    try (final ResultSet rs = database.query("sql",
        "SELECT expand(u) FROM (MATCH {rid: :rid, as: u} RETURN u)", Map.of("rid", rid))) {
      assertThat(rs.hasNext()).isTrue();
      final Result item = rs.next();
      assertThat(item.<String>getProperty("name")).isEqualTo("n1");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void parameterizedRidAsTraversalRoot() {
    final RID rid = ridOf("n1");

    try (final ResultSet rs = database.query("sql",
        "MATCH {rid: :rid, as: u}-Friend->{as: x} RETURN x.name AS name ORDER BY name", Map.of("rid", rid))) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("n2");
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("n3");
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
