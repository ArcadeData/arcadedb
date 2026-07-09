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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #5109.
 * <p>
 * {@code exists((a)-[:R {w: 999}]->(:X))} must enforce the inline relationship property
 * filter {@code {w: 999}}. A non-matching value must yield {@code false}, not {@code true}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5109ExistsRelPropertyTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-5109-exists-rel-property").create();
    database.transaction(() -> {
      // Alice -[:R {w: 3}]-> Bob, Alice -[:R {w: 5}]-> Dave. No w=999 anywhere.
      database.command("opencypher", "CREATE "
          + "(a:X {name: 'Alice'}),"
          + "(b:X {name: 'Bob'}),"
          + "(d:X {name: 'Dave'}),"
          + "(a)-[:R {w: 3}]->(b),"
          + "(a)-[:R {w: 5}]->(d)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private boolean existsBool(final String existsPattern) {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:X {name: 'Alice'}) RETURN exists(" + existsPattern + ") AS v");
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    return Boolean.TRUE.equals(r.getProperty("v"));
  }

  @Test
  void truthTableForInlineRelPropertyFilter() {
    assertThat(existsBool("(a)-[:R]->(:X)")).as("any_R").isTrue();
    assertThat(existsBool("(a)-[:R {w: 3}]->(:X)")).as("has_w3").isTrue();
    assertThat(existsBool("(a)-[:R {w: 5}]->(:X)")).as("has_w5").isTrue();
    assertThat(existsBool("(a)-[:R {w: 999}]->(:X)")).as("has_w999").isFalse();
  }

  @Test
  void whereExistsWithNonMatchingRelPropertyReturnsEmpty() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:X {name: 'Alice'}) WHERE exists((a)-[:R {w: 999}]->(:X)) RETURN a.name AS name");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void whereExistsWithMatchingRelPropertyReturnsRow() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:X {name: 'Alice'}) WHERE exists((a)-[:R {w: 3}]->(:X)) RETURN a.name AS name");
    assertThat(rs.hasNext()).isTrue();
    assertThat((String) rs.next().getProperty("name")).isEqualTo("Alice");
  }

  @Test
  void whereExistsWithBoundEndNodeEnforcesRelProperty() {
    // Bob is the end of the w=3 edge; a w=999 filter to a bound Bob must not match.
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:X {name: 'Alice'}), (b:X {name: 'Bob'}) "
            + "WHERE exists((a)-[:R {w: 999}]->(b)) RETURN a.name AS name");
    assertThat(rs.hasNext()).isFalse();

    final ResultSet rs2 = database.query("opencypher",
        "MATCH (a:X {name: 'Alice'}), (b:X {name: 'Bob'}) "
            + "WHERE exists((a)-[:R {w: 3}]->(b)) RETURN a.name AS name");
    assertThat(rs2.hasNext()).isTrue();
  }

  @Test
  void notExistsWithNonMatchingRelPropertyReturnsRow() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:X {name: 'Alice'}) WHERE NOT exists((a)-[:R {w: 999}]->(:X)) RETURN a.name AS name");
    assertThat(rs.hasNext()).isTrue();
    assertThat((String) rs.next().getProperty("name")).isEqualTo("Alice");
  }
}
